using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Collections.Concurrent;
using MediaPager.App.PluginContracts;

namespace MediaPager.Plugins.Metadata.MusicBrainz;

/// <summary>Looks up recordings by known title/artist text or a MusicBrainz recording id.
/// MusicBrainz is a metadata database; this provider does not accept or fingerprint audio.</summary>
public sealed class MusicBrainzProviderPlugin : IMusicMetadataProviderPlugin
{
    public const string PluginId = "mediapager.metadata.musicbrainz";
    private const string ApiRoot = "https://musicbrainz.org/ws/2/";
    private static readonly TimeSpan MinimumRequestInterval = TimeSpan.FromSeconds(1);
    private static readonly SemaphoreSlim ProcessRateGate = new(1, 1);
    private static readonly string RateLockPath = Path.Combine(Path.GetTempPath(), "mediapager-musicbrainz-rate.lock");
    private static readonly ConcurrentDictionary<string, CacheEntry> ResponseCache = new(StringComparer.Ordinal);
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(20) };

    public PluginDescriptor Descriptor { get; } = new(
        PluginId,
        "MusicBrainz",
        "0.1.0",
        "MediaPager",
        "Keyless music recording and release metadata lookup.");

    public bool Handles(MediaKind kind) => kind == MediaKind.Music;

    public async Task<TitleDetails?> GetDetailsAsync(
        MediaKind kind,
        string externalId,
        CancellationToken cancellationToken)
    {
        if (!Handles(kind) || !Guid.TryParse(externalId, out var recordingId)) return null;
        var recording = await GetRecordingByIdAsync(recordingId.ToString(), cancellationToken);
        return recording is null ? null : ToDetails(recording);
    }

    public async Task<EnrichmentData?> EnrichAsync(
        string externalId,
        MediaKind kind,
        CancellationToken cancellationToken)
    {
        if (!Handles(kind) || !Guid.TryParse(externalId, out var recordingId)) return null;
        var recording = await GetRecordingByIdAsync(recordingId.ToString(), cancellationToken);
        return recording is null ? null : ToEnrichment(recording);
    }

    public async Task<EnrichmentData?> EnrichTrackAsync(
        MusicTrackLookupRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.Title)) return null;
        if (!string.IsNullOrWhiteSpace(request.Isrc) && IsValidIsrc(request.Isrc))
        {
            var byIsrc = await SearchRecordingsAsync($"isrc:{EscapeQueryValue(request.Isrc)}", cancellationToken);
            if (byIsrc.Count > 0) return ToEnrichment(byIsrc[0]);
        }

        var query = $"recording:\"{EscapeQueryValue(request.Title)}\"";
        var artist = request.Artists.FirstOrDefault()?.Name;
        if (!string.IsNullOrWhiteSpace(artist)) query += $" AND artist:\"{EscapeQueryValue(artist)}\"";
        if (!string.IsNullOrWhiteSpace(request.Album)) query += $" AND release:\"{EscapeQueryValue(request.Album)}\"";

        var recordings = await SearchRecordingsAsync(query, cancellationToken);
        var best = recordings
            .Select(recording => (Recording: recording, Score: MatchScore(request, recording)))
            .OrderByDescending(candidate => candidate.Score)
            .FirstOrDefault();
        return best.Recording is null || best.Score < 3 ? null : ToEnrichment(best.Recording);
    }

    private static async Task<IReadOnlyList<Recording>> SearchRecordingsAsync(
        string query,
        CancellationToken cancellationToken)
    {
        var cacheKey = $"search:{query.Trim().ToLowerInvariant()}";
        if (TryGetCached(cacheKey, out var cached)) return cached;

        var encoded = Uri.EscapeDataString(query);
        var uri = new Uri($"{ApiRoot}recording/?query={encoded}&fmt=json&limit=8&inc=artist-credits+releases+isrcs+media+tags");
        using var document = await GetJsonAsync(uri, cancellationToken);
        if (document is null || !document.RootElement.TryGetProperty("recordings", out var values) || values.ValueKind != JsonValueKind.Array)
        {
            Cache(cacheKey, []);
            return [];
        }
        var recordings = values.EnumerateArray().Select(ParseRecording).Where(recording => recording is not null).Cast<Recording>().ToArray();
        Cache(cacheKey, recordings);
        return recordings;
    }

    private static async Task<Recording?> GetRecordingByIdAsync(string recordingId, CancellationToken cancellationToken)
    {
        var cacheKey = $"recording:{recordingId.ToLowerInvariant()}";
        if (TryGetCached(cacheKey, out var cached)) return cached.FirstOrDefault();

        var uri = new Uri($"{ApiRoot}recording/{Uri.EscapeDataString(recordingId)}?fmt=json&inc=artist-credits+releases+isrcs+media+tags");
        using var document = await GetJsonAsync(uri, cancellationToken);
        var recording = document is null ? null : ParseRecording(document.RootElement);
        Cache(cacheKey, recording is null ? [] : [recording]);
        return recording;
    }

    private static bool TryGetCached(string key, out IReadOnlyList<Recording> recordings)
    {
        if (ResponseCache.TryGetValue(key, out var entry))
        {
            if (entry.ExpiresAt > DateTimeOffset.UtcNow)
            {
                recordings = entry.Recordings;
                return true;
            }
            ResponseCache.TryRemove(key, out _);
        }
        recordings = [];
        return false;
    }

    private static void Cache(string key, IReadOnlyList<Recording> recordings)
    {
        ResponseCache[key] = new CacheEntry(
            recordings,
            DateTimeOffset.UtcNow + (recordings.Count == 0 ? TimeSpan.FromMinutes(20) : TimeSpan.FromHours(24)));
        if (ResponseCache.Count <= 4096) return;
        foreach (var cached in ResponseCache.Where(pair => pair.Value.ExpiresAt <= DateTimeOffset.UtcNow).Take(512))
            ResponseCache.TryRemove(cached.Key, out _);
    }

    private static async Task<JsonDocument?> GetJsonAsync(Uri uri, CancellationToken cancellationToken)
    {
        await ProcessRateGate.WaitAsync(cancellationToken);
        try
        {
            await using var rateLock = await AcquireRateLockAsync(cancellationToken);
            var lastRequest = await ReadLastRequestAsync(rateLock, cancellationToken);
            var nextAllowed = lastRequest + MinimumRequestInterval;
            var delay = nextAllowed - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero) await Task.Delay(delay, cancellationToken);

            await WriteLastRequestAsync(rateLock, DateTimeOffset.UtcNow, cancellationToken);
            using var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.UserAgent.ParseAdd("MediaPager/1.0 (https://github.com/MediaPager)");
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            using var response = await Http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            if (response.StatusCode == System.Net.HttpStatusCode.NotFound) return null;
            response.EnsureSuccessStatusCode();
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        }
        finally
        {
            ProcessRateGate.Release();
        }
    }

    private static async Task<FileStream> AcquireRateLockAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return new FileStream(RateLockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 128, FileOptions.Asynchronous);
            }
            catch (IOException)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(75), cancellationToken);
            }
        }
    }

    private static async Task<DateTimeOffset> ReadLastRequestAsync(FileStream stream, CancellationToken cancellationToken)
    {
        if (stream.Length == 0) return DateTimeOffset.MinValue;
        stream.Position = 0;
        var bytes = new byte[stream.Length];
        var read = await stream.ReadAsync(bytes, cancellationToken);
        return long.TryParse(System.Text.Encoding.UTF8.GetString(bytes, 0, read), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var milliseconds)
            ? DateTimeOffset.FromUnixTimeMilliseconds(milliseconds)
            : DateTimeOffset.MinValue;
    }

    private static async Task WriteLastRequestAsync(FileStream stream, DateTimeOffset timestamp, CancellationToken cancellationToken)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(timestamp.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture));
        stream.Position = 0;
        stream.SetLength(0);
        await stream.WriteAsync(bytes, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    private static Recording? ParseRecording(JsonElement item)
    {
        var id = ReadString(item, "id");
        var title = ReadString(item, "title");
        if (string.IsNullOrWhiteSpace(id) || string.IsNullOrWhiteSpace(title)) return null;

        var artists = ReadCredits(item, "artist-credit");
        var isrc = ReadStringArray(item, "isrcs").FirstOrDefault();
        var release = item.TryGetProperty("releases", out var releases) && releases.ValueKind == JsonValueKind.Array
            ? releases.EnumerateArray().FirstOrDefault()
            : default;
        var album = release.ValueKind == JsonValueKind.Object ? ReadString(release, "title") : null;
        var albumArtists = release.ValueKind == JsonValueKind.Object ? ReadCredits(release, "artist-credit") : artists;
        if (albumArtists.Count == 0) albumArtists = artists;
        var releaseId = release.ValueKind == JsonValueKind.Object ? ReadString(release, "id") : null;
        var releaseDate = release.ValueKind == JsonValueKind.Object ? ReadString(release, "date") : null;
        var (trackNumber, trackCount, discNumber, discCount) = FindTrackPosition(release, id);
        var genres = ReadNamedValues(item, "genres");
        if (genres.Count == 0) genres = ReadNamedValues(item, "tags");

        return new Recording(
            id,
            title,
            ReadString(item, "disambiguation"),
            artists,
            album,
            albumArtists,
            releaseId,
            isrc,
            ParseYear(releaseDate),
            trackNumber,
            trackCount,
            discNumber,
            discCount,
            genres);
    }

    private static IReadOnlyList<MusicArtistCredit> ReadCredits(JsonElement parent, string property)
    {
        if (!parent.TryGetProperty(property, out var values) || values.ValueKind != JsonValueKind.Array) return [];
        var credits = new List<MusicArtistCredit>();
        foreach (var value in values.EnumerateArray())
        {
            if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty("artist", out var artist)) continue;
            var name = ReadString(value, "name") ?? ReadString(artist, "name");
            if (!string.IsNullOrWhiteSpace(name)) credits.Add(new MusicArtistCredit(name, ReadString(artist, "id")));
        }
        return credits;
    }

    private static (int? Track, int? TrackCount, int? Disc, int? DiscCount) FindTrackPosition(JsonElement release, string recordingId)
    {
        if (release.ValueKind != JsonValueKind.Object || !release.TryGetProperty("media", out var media) || media.ValueKind != JsonValueKind.Array)
            return (null, null, null, null);
        var disc = 0;
        foreach (var medium in media.EnumerateArray())
        {
            disc++;
            var discCount = media.GetArrayLength();
            var count = ReadInt(medium, "track-count");
            if (!medium.TryGetProperty("tracks", out var tracks) || tracks.ValueKind != JsonValueKind.Array) continue;
            foreach (var track in tracks.EnumerateArray())
            {
                var recording = track.TryGetProperty("recording", out var nested) ? nested : default;
                if (ReadString(recording, "id") == recordingId)
                    return (ReadInt(track, "position"), count ?? tracks.GetArrayLength(), disc, discCount);
            }
        }
        return (null, null, null, null);
    }

    private static IReadOnlyList<string> ReadNamedValues(JsonElement parent, string property)
    {
        if (!parent.TryGetProperty(property, out var values) || values.ValueKind != JsonValueKind.Array) return [];
        return values.EnumerateArray()
            .Select(value => ReadString(value, "name"))
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Cast<string>()
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static IReadOnlyList<string> ReadStringArray(JsonElement parent, string property) =>
        parent.TryGetProperty(property, out var values) && values.ValueKind == JsonValueKind.Array
            ? values.EnumerateArray().Where(value => value.ValueKind == JsonValueKind.String).Select(value => value.GetString()!).ToArray()
            : [];

    private static string? ReadString(JsonElement parent, string property) =>
        parent.ValueKind == JsonValueKind.Object && parent.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static int? ReadInt(JsonElement parent, string property) =>
        parent.ValueKind == JsonValueKind.Object && parent.TryGetProperty(property, out var value) && value.TryGetInt32(out var number)
            ? number
            : null;

    private static EnrichmentData ToEnrichment(Recording recording) => new(
        recording.Title,
        OriginalTitle: recording.Title,
        Year: recording.Year,
        Genres: recording.Genres,
        Credits: recording.Artists.Select(credit => new CreditRole(credit.Name, "Artist")).ToArray())
    {
        Music = new MusicTrackMetadata(
            MusicBrainzRecordingId: recording.Id,
            MusicBrainzReleaseId: recording.ReleaseId,
            Isrc: recording.Isrc,
            Album: recording.Album,
            Artists: recording.Artists,
            AlbumArtists: recording.AlbumArtists,
            TrackNumber: recording.TrackNumber,
            TrackCount: recording.TrackCount,
            DiscNumber: recording.DiscNumber,
            DiscCount: recording.DiscCount,
            Year: recording.Year,
            Genres: recording.Genres),
    };

    private static TitleDetails ToDetails(Recording recording) => new(
        ExternalId: recording.Id,
        Kind: MediaKind.Music,
        Title: recording.Title,
        Overview: string.IsNullOrWhiteSpace(recording.Disambiguation) ? null : recording.Disambiguation,
        Year: recording.Year,
        OriginalTitle: recording.Title,
        Credits: recording.Artists.Select(credit => new CreditRole(credit.Name, "Artist")).ToArray())
    {
        Metadata = new ContentMetadata?[]
        {
            recording.Album is null ? null : new ContentMetadata("Album", recording.Album),
            recording.Isrc is null ? null : new ContentMetadata("ISRC", recording.Isrc),
            new ContentMetadata("MusicBrainz recording ID", recording.Id),
        }.Where(value => value is not null).Cast<ContentMetadata>().ToArray(),
    };

    private static int MatchScore(MusicTrackLookupRequest request, Recording recording)
    {
        var score = 0;
        var queryTitle = Normalize(request.Title);
        var resultTitle = Normalize(recording.Title);
        if (queryTitle == resultTitle) score += 8;
        else if (resultTitle.Contains(queryTitle, StringComparison.Ordinal) || queryTitle.Contains(resultTitle, StringComparison.Ordinal)) score += 4;

        var queryArtists = request.Artists.Select(credit => Normalize(credit.Name)).Where(value => value.Length > 0).ToArray();
        if (queryArtists.Length > 0 && recording.Artists.Any(credit => queryArtists.Contains(Normalize(credit.Name), StringComparer.Ordinal))) score += 6;
        if (!string.IsNullOrWhiteSpace(request.Album) && !string.IsNullOrWhiteSpace(recording.Album))
        {
            var queryAlbum = Normalize(request.Album);
            var resultAlbum = Normalize(recording.Album);
            if (queryAlbum == resultAlbum) score += 4;
            else if (queryAlbum.Contains(resultAlbum, StringComparison.Ordinal) || resultAlbum.Contains(queryAlbum, StringComparison.Ordinal)) score += 2;
        }
        if (!string.IsNullOrWhiteSpace(request.Isrc) && string.Equals(request.Isrc, recording.Isrc, StringComparison.OrdinalIgnoreCase)) score += 100;
        return score;
    }

    private static string Normalize(string value) => new(value.Where(char.IsLetterOrDigit).Select(char.ToLowerInvariant).ToArray());

    private static string EscapeQueryValue(string value)
    {
        const string special = "+-!(){}[]^\"~*?:\\/";
        return string.Concat(value.Select(character => special.Contains(character) ? $"\\{character}" : character.ToString()));
    }

    private static bool IsValidIsrc(string value) =>
        value.Length == 12 && value.Take(2).All(char.IsAsciiLetter) && value.Skip(2).Take(3).All(char.IsAsciiLetterOrDigit) && value.Skip(5).All(char.IsAsciiDigit);

    private static int? ParseYear(string? value) =>
        value is { Length: >= 4 } && int.TryParse(value.AsSpan(0, 4), NumberStyles.None, CultureInfo.InvariantCulture, out var year)
            ? year
            : null;

    private sealed record Recording(
        string Id,
        string Title,
        string? Disambiguation,
        IReadOnlyList<MusicArtistCredit> Artists,
        string? Album,
        IReadOnlyList<MusicArtistCredit> AlbumArtists,
        string? ReleaseId,
        string? Isrc,
        int? Year,
        int? TrackNumber,
        int? TrackCount,
        int? DiscNumber,
        int? DiscCount,
        IReadOnlyList<string> Genres);

    private sealed record CacheEntry(IReadOnlyList<Recording> Recordings, DateTimeOffset ExpiresAt);
}
