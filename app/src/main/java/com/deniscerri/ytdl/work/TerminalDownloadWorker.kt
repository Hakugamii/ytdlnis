package com.deniscerri.ytdl.work

import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.util.Log
import android.util.Patterns
import android.widget.Toast
import androidx.preference.PreferenceManager
import androidx.work.CoroutineWorker
import androidx.work.ForegroundInfo
import androidx.work.WorkerParameters
import com.deniscerri.ytdl.R
import com.deniscerri.ytdl.database.DBManager
import com.deniscerri.ytdl.database.models.Format
import com.deniscerri.ytdl.database.models.LogItem
import com.deniscerri.ytdl.database.repository.LogRepository
import com.deniscerri.ytdl.database.viewmodel.DownloadViewModel
import com.deniscerri.ytdl.ui.more.terminal.TerminalActivity
import com.deniscerri.ytdl.util.FileUtil
import com.deniscerri.ytdl.util.NotificationUtil
import com.deniscerri.ytdl.util.extractors.ytdlp.YTDLPUtil
import com.yausername.youtubedl_android.YoutubeDL
import com.yausername.youtubedl_android.YoutubeDLRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.greenrobot.eventbus.EventBus
import java.io.File


class TerminalDownloadWorker(
    private val context: Context,
    workerParams: WorkerParameters
) : CoroutineWorker(context, workerParams) {
    override suspend fun doWork(): Result {
        itemId = inputData.getInt("id", 0)
        val dao = DBManager.getInstance(context).terminalDao
        if (itemId == 0) return Result.failure()

        // Read command from database (for retries with updated URL) or inputData (first run)
        val terminalItem = dao.getTerminalById(itemId.toLong())
        var command = terminalItem?.command ?: inputData.getString("command")
        if (command.isNullOrEmpty()) return Result.failure()

        // Check if command has been updated to use batch file (indicated by -a option)
        val actualCommand = command

        Log.d(TAG, "Executing command: $actualCommand")

        val dbManager = DBManager.getInstance(context)
        val logRepo = LogRepository(dbManager.logDao)
        val notificationUtil = NotificationUtil(context)
        val handler = Handler(Looper.getMainLooper())

        // Track replacements from preprocessing to mark files in summary (replacement videoId -> original videoId)
        val replacementMap = mutableMapOf<String, String>()

        // Load existing replacement map from file (for retries)
        val replacementMapFile = File(context.cacheDir, "replacements_${itemId}.json")
        if (replacementMapFile.exists()) {
            try {
                val jsonContent = replacementMapFile.readText()
                // Simple JSON parsing: {"replacementId":"originalId",...}
                val entries = jsonContent.trim().removeSurrounding("{", "}").split(",")
                entries.forEach { entry ->
                    if (entry.isNotBlank()) {
                        val parts = entry.split(":")
                        if (parts.size == 2) {
                            val key = parts[0].trim().removeSurrounding("\"")
                            val value = parts[1].trim().removeSurrounding("\"")
                            replacementMap[key] = value
                            Log.d(TAG, "Loaded replacement from file: $key -> $value")
                        }
                    }
                }
            } catch (e: Exception) {
                Log.w(TAG, "Failed to load replacement map: ${e.message}")
            }
        }

        val intent = Intent(context, TerminalActivity::class.java)
        val pendingIntent = PendingIntent.getActivity(context, 0, intent, PendingIntent.FLAG_IMMUTABLE)
        val notification = notificationUtil.createDownloadServiceNotification(pendingIntent, actualCommand.take(65), NotificationUtil.DOWNLOAD_TERMINAL_RUNNING_NOTIFICATION_ID)
        if (Build.VERSION.SDK_INT >= 33) {
            setForegroundAsync(ForegroundInfo(itemId, notification, FOREGROUND_SERVICE_TYPE_DATA_SYNC))
        }else{
            setForegroundAsync(ForegroundInfo(itemId, notification))
        }

        val request = YoutubeDLRequest(emptyList())
        val sharedPreferences =  PreferenceManager.getDefaultSharedPreferences(context)

        val downloadLocation = sharedPreferences.getString("command_path", FileUtil.getDefaultCommandPath())
        request.addOption(
            "--config-locations",
            File(context.cacheDir.absolutePath + "/config-TERMINAL[${System.currentTimeMillis()}].txt").apply {
                writeText(actualCommand)
            }.absolutePath
        )

        if (sharedPreferences.getBoolean("use_cookies", false)){
            FileUtil.getCookieFile(context){
                request.addOption("--cookies", it)
            }

            val useHeader = sharedPreferences.getBoolean("use_header", false)
            val header = sharedPreferences.getString("useragent_header", "")
            if (useHeader && !header.isNullOrBlank()){
                request.addOption("--add-header","User-Agent:${header}")
            }
        }

        val commandPath = sharedPreferences.getString("command_path", FileUtil.getDefaultCommandPath())!!
        var noCache = !sharedPreferences.getBoolean("cache_downloads", true) && File(FileUtil.formatPath(commandPath)).canWrite()

        // If command explicitly sets path with -P, don't override it
        if (actualCommand.contains("-P ")) {
            noCache = true
            Log.d(TAG, "Command contains explicit -P path, using it directly")
        } else {
            // Set download path based on cache preference
            if (!noCache) {
                // Use cache folder, will move files later
                request.addOption("-P", FileUtil.getCachePath(context) + "TERMINAL/" + itemId)
                Log.d(TAG, "Using cache path: ${FileUtil.getCachePath(context)}TERMINAL/$itemId")
            } else {
                // Download directly to final destination
                request.addOption("-P", FileUtil.formatPath(commandPath))
                Log.d(TAG, "Downloading directly to: ${FileUtil.formatPath(commandPath)}")
            }
        }





        val logDownloads = sharedPreferences.getBoolean("log_downloads", false) && !sharedPreferences.getBoolean("incognito", false)

        val initialLogDetails = "Terminal Task\n" +
                "Command:\n${actualCommand.trim()}\n\n"
        val logItem = LogItem(
            0,
            "Terminal Task",
            initialLogDetails,
            Format(),
            DownloadViewModel.Type.command,
            System.currentTimeMillis(),
        )

        val eventBus = EventBus.getDefault()

        // Track the album being injected via FFmpeg postprocessor
        // This will be used as fallback for files where we don't detect album from yt-dlp output
        var injectedAlbum: String? = null

        // **PRE-PROCESS PLAYLISTS TO DETECT UNAVAILABLE VIDEOS BEFORE DOWNLOADING**
        val isPlaylistUrl = actualCommand.contains("playlist?list=")
        if (isPlaylistUrl) {
            Log.i(TAG, "Detected playlist URL - checking for unavailable videos before download...")

            // Show status in terminal
            runBlocking {
                dao.updateLog("Scanning playlist for unavailable videos...\nThis may take a moment...", itemId.toLong())
            }

            // Extract playlist URL from command
            val playlistMatch = Regex("""(https?://[^\s]+playlist\?list=[^&\s]+)""").find(actualCommand)
            if (playlistMatch != null) {
                val playlistUrl = playlistMatch.value
                Log.i(TAG, "Preprocessing playlist: $playlistUrl")

                val commandTemplateDao = DBManager.getInstance(context).commandTemplateDao
                val ytdlpUtil = YTDLPUtil(context, commandTemplateDao)

                val preprocessResult = runCatching {
                    runBlocking {
                        ytdlpUtil.preprocessPlaylistForUnavailableVideos(playlistUrl) { current, total, message ->
                            // Update terminal log with progress
                            runBlocking {
                                val progressMessage = "[$current/$total] $message"
                                dao.updateLog(progressMessage, itemId.toLong())
                            }
                        }
                    }
                }.getOrNull()

                if (preprocessResult != null) {
                    val (processedUrls, replacements) = preprocessResult

                    // Store replacements for tracking in summary (replacement ID -> original ID)
                    replacements.forEach { (originalId, replacementId) ->
                        replacementMap[replacementId] = originalId
                        Log.d(TAG, "Tracked replacement for summary: $replacementId replaces $originalId")
                    }

                    // Save replacement map to file for use during download
                    if (replacementMap.isNotEmpty()) {
                        val replacementMapFile = File(context.cacheDir, "replacements_${itemId}.json")
                        try {
                            val jsonContent = replacementMap.entries.joinToString(",", "{", "}") { (k, v) ->
                                "\"$k\":\"$v\""
                            }
                            replacementMapFile.writeText(jsonContent)
                            Log.d(TAG, "Saved replacement map to file after preprocessing")
                        } catch (e: Exception) {
                            Log.w(TAG, "Failed to save replacement map: ${e.message}")
                        }
                    }

                    // ALWAYS extract playlist title for metadata, even if no unavailable videos
                    val playlistTitle = runCatching {
                        runBlocking {
                            // Get playlist info (not individual videos)
                            val infoRequest = YoutubeDLRequest(playlistUrl)
                            infoRequest.addOption("--flat-playlist")
                            infoRequest.addOption("--print", "%(playlist_title)s")
                            val response = YoutubeDL.getInstance().execute(infoRequest)
                            val rawTitle = response.out.trim()

                            // CRITICAL: Sanitize playlist title to remove newlines and duplicates
                            // YouTube sometimes returns titles with embedded newlines or repeated content
                            val title = rawTitle
                                .lines()                                    // Split by newlines
                                .firstOrNull { it.isNotBlank() }           // Take only first non-blank line
                                ?.replace("\r", "")                        // Remove carriage returns
                                ?.replace(Regex("\\s+"), " ")              // Normalize whitespace
                                ?.trim()                                   // Trim edges
                                ?.takeIf { it.isNotBlank() }               // Ensure not empty

                            Log.i(TAG, "Extracted playlist title: '$title' (sanitized from raw: '${rawTitle.replace("\n", "\\n").take(100)}')")
                            title
                        }
                    }.getOrNull()

                    if (replacements.isNotEmpty()) {
                        Log.i(TAG, "Pre-processing found ${replacements.size} unavailable video(s)")
                        replacements.forEach { (original, replacement) ->
                            Log.i(TAG, "  $original → $replacement")
                        }
                    } else {
                        Log.i(TAG, "All videos in playlist are available")
                    }

                    // Create batch file with processed URLs (always, for consistency)
                    val urlFile = File(context.cacheDir, "playlist_preprocessed_${itemId}.txt")
                    urlFile.writeText(processedUrls.joinToString("\n"))
                    Log.i(TAG, "Created preprocessed URL list: ${urlFile.absolutePath} (${processedUrls.size} videos)")

                    // Update command to use batch file instead of playlist URL
                    // Remove the full playlist URL including any query parameters (&si=, &feature=, etc.)
                    val playlistUrlWithParams = Regex("""(https?://[^\s]*playlist\?[^\s]*)""").find(actualCommand)?.value ?: playlistUrl

                    // Build updated command with playlist metadata
                    val updatedCommand = if (playlistTitle != null) {
                        // Escape special characters in playlist title for shell command
                        val escapedTitle = playlistTitle.replace("\"", "\\\"").replace("'", "'\\''")

                        // Find where to inject the album metadata
                        val beforeUrl = actualCommand.substringBefore(playlistUrlWithParams)
                        val afterUrl = actualCommand.substringAfter(playlistUrlWithParams)

                        var modifiedBefore = beforeUrl
                        var modifiedAfter = afterUrl

                        // Replace output template to avoid duplication: use only %(title)s without %(uploader)s
                        // This prevents "Kendrick Lamar - Kendrick Lamar - Song.opus"
                        if (modifiedBefore.contains("-o \"%(uploader)")) {
                            modifiedBefore = modifiedBefore.replace(
                                Regex("""-o\s+"%(uploader)[^"]+"""),
                                """-o "%(title).200B.%(ext)s""""
                            )
                            Log.i(TAG, "Modified output template to use title only (avoid duplication)")
                        }

                        // CRITICAL: Remove the conflicting parse-metadata rule that tries to use %(playlist_title)s
                        // This doesn't work with batch files because playlist context is lost
                        if (modifiedAfter.contains("--parse-metadata \"%(album,playlist_title,playlist|)s:%(meta_album)s\"")) {
                            modifiedAfter = modifiedAfter.replace("--parse-metadata \"%(album,playlist_title,playlist|)s:%(meta_album)s\" ", "")
                            Log.i(TAG, "Removed conflicting playlist_title parse-metadata rule")
                        }

                        // Also remove from modifiedBefore if it's there
                        if (modifiedBefore.contains("--parse-metadata \"%(album,playlist_title,playlist|)s:%(meta_album)s\"")) {
                            modifiedBefore = modifiedBefore.replace("--parse-metadata \"%(album,playlist_title,playlist|)s:%(meta_album)s\" ", "")
                            Log.i(TAG, "Removed conflicting playlist_title parse-metadata rule from before URL")
                        }

                        // Strategy: Use --postprocessor-args to set album with FFmpeg during audio extraction
                        // This works for ALL files because FFmpeg creates the metadata, not just replaces it
                        // We add it to ExtractAudio postprocessor which runs for all audio conversions
                        val escapedAlbum = escapedTitle.replace("\"", "\\\"")
                        val albumForce = """--postprocessor-args "ExtractAudio:-metadata album=$escapedAlbum" """

                        // Add album metadata injection right before the batch file option
                        """$modifiedBefore$albumForce-a ${urlFile.absolutePath} $modifiedAfter"""
                    } else {
                        // No playlist title - just use batch file
                        actualCommand.replace(playlistUrlWithParams, "-a ${urlFile.absolutePath}")
                    }

                    if (playlistTitle != null) {
                        Log.i(TAG, "Injected playlist title as album: $playlistTitle")
                        injectedAlbum = playlistTitle // Store for use as fallback
                    }

                    Log.i(TAG, "Updated command with batch file (replaced: $playlistUrlWithParams)")

                    // Build status message for terminal
                    val statusMessage = buildString {
                        appendLine("Playlist preprocessing complete!")
                        appendLine()
                        appendLine("Summary:")
                        appendLine("  • Total videos: ${processedUrls.size}")
                        appendLine("  • Unavailable videos found: ${replacements.size}")
                        appendLine("  • Replacements made: ${replacements.size}")
                        if (replacements.isNotEmpty()) {
                            appendLine()
                            appendLine("Replacements:")
                            replacements.forEach { (original, replacement) ->
                                appendLine("  • $original → $replacement")
                            }
                        }
                        appendLine()
                        appendLine("Starting download with ${processedUrls.size} videos...")
                    }

                    // Store batch file path in the terminal item for retry
                    runBlocking {
                        // Update command to use batch file
                        dao.updateCommand(updatedCommand, itemId.toLong())
                        dao.updateLog(statusMessage, itemId.toLong())
                    }

                    handler.postDelayed({
                        val msg = if (replacements.isNotEmpty()) {
                            "Playlist preprocessed: ${replacements.size} unavailable video(s) replaced"
                        } else {
                            "Playlist preprocessed: All videos available"
                        }
                        Toast.makeText(context, msg, Toast.LENGTH_LONG).show()
                    }, 100)

                    notificationUtil.cancelDownloadNotification(itemId)
                    // Return retry - the worker will restart and use the batch file
                    return Result.retry()
                } else {
                    Log.w(TAG, "Failed to preprocess playlist, will use fallback during download if needed")
                    runBlocking {
                        dao.updateLog("Could not preprocess playlist\nWill check videos during download instead...\n\nStarting download...", itemId.toLong())
                    }
                }
            }
        }

        // Track unavailable videos during execution (fallback if preprocessing missed any)
        val unavailableVideoIds = mutableSetOf<String>()
        // Track videos that failed without fallbacks (for summary)
        val failedVideos = mutableMapOf<String, String>() // Map of video ID to original URL
        // Pattern to match: ERROR: [youtube] Xl9xXuHHkoc: Video unavailable
        val videoIdPattern = Regex("""ERROR:.*?\[youtube].*?([a-zA-Z0-9_-]{11}).*?(?:Video unavailable|unavailable)""", RegexOption.IGNORE_CASE)
        var shouldKillProcess = false
        var killProcessReason = ""

        // Track downloaded files for summary
        data class DownloadedFileInfo(
            val filename: String,
            val artist: String?,
            val title: String?,
            val album: String?,
            val videoId: String?,
            val isFallback: Boolean
        )
        val downloadedFiles = mutableListOf<DownloadedFileInfo>()
        var currentVideoId: String? = null
        var currentFileInfo = DownloadedFileInfo("", null, null, null, null, false)


        // Re-extract album from FFmpeg postprocessor args if present
        // This is needed when the worker retries after preprocessing - injectedAlbum is reset
        // Pattern: --postprocessor-args "ExtractAudio:-metadata album=ALBUMNAME"
        val ffmpegAlbumMatch = Regex("""--postprocessor-args\s+"ExtractAudio:-metadata album=([^"]+)"""").find(actualCommand)
        if (ffmpegAlbumMatch != null) {
            injectedAlbum = ffmpegAlbumMatch.groupValues[1]
            Log.i(TAG, "📀 Re-extracted injected album from FFmpeg args: '$injectedAlbum'")
        }

        // Extract album from --replace-in-metadata command if present (for diagnostic logging)
        val replacedAlbum: String? = Regex("""--replace-in-metadata\s+"album"\s+".*"\s+"([^"]+)"""").find(actualCommand)?.groupValues?.get(1)
        if (replacedAlbum != null) {
            Log.i(TAG, "📀 Detected album replacement in command: '$replacedAlbum'")
            Log.i(TAG, "⚠️ Note: --replace-in-metadata only replaces existing album fields.")
        }

        kotlin.runCatching {
            if (logDownloads){
                runBlocking {
                    logItem.id = logRepo.insert(logItem)
                }
            }

            Log.i(TAG, "Starting execution with unavailable video detection enabled")

            YoutubeDL.getInstance().execute(request, itemId.toString(), true){ progress, _, line ->
                // Monitor for unavailable videos in real-time
                if (line.contains("ERROR", ignoreCase = true) && line.contains("youtube", ignoreCase = true)) {
                    Log.w(TAG, "Error line detected: $line")

                    // Try to extract video ID from error line
                    videoIdPattern.find(line)?.let { match ->
                        val videoId = match.groupValues[1]
                        if (unavailableVideoIds.add(videoId)) {
                            Log.e(TAG, "DETECTED UNAVAILABLE VIDEO: $videoId - KILLING PROCESS TO START FALLBACK")
                            shouldKillProcess = true
                            killProcessReason = "Detected unavailable video: $videoId"

                            // Kill the yt-dlp process immediately so we can run fallback
                            try {
                                YoutubeDL.getInstance().destroyProcessById(itemId.toString())
                                Log.i(TAG, "Successfully killed yt-dlp process")
                            } catch (e: Exception) {
                                Log.e(TAG, "Failed to kill process: ${e.message}")
                            }
                        }
                    }
                }

                // Track current video being processed - extract video ID from URL in yt-dlp output
                // Pattern: [youtube] Extracting URL: https://www.youtube.com/watch?v=GF8aaTu2kg0
                if (line.contains("[youtube] Extracting URL:")) {
                    // Save previous file info if complete (has filename = file was written)
                    if (currentFileInfo.filename.isNotEmpty()) {
                        // Check if we already added this file (to avoid duplicates)
                        val alreadyAdded = downloadedFiles.any { it.filename == currentFileInfo.filename && it.videoId == currentFileInfo.videoId }

                        if (!alreadyAdded) {
                            // Apply fallbacks for missing metadata
                            var finalTitle = currentFileInfo.title
                            if (finalTitle == null) {
                                val nameWithoutExt = currentFileInfo.filename.substringBeforeLast(".")
                                finalTitle = if (nameWithoutExt.contains(" - ")) {
                                    nameWithoutExt.substringAfter(" - ")
                                } else {
                                    nameWithoutExt
                                }
                                Log.d(TAG, "Extracted title from filename on video transition: $finalTitle")
                            }

                            // Use injectedAlbum as fallback if album wasn't detected from yt-dlp output
                            val finalAlbum = currentFileInfo.album ?: injectedAlbum

                            val finalFileInfo = currentFileInfo.copy(title = finalTitle, album = finalAlbum)
                            downloadedFiles.add(finalFileInfo)
                            Log.i(TAG, "Added file on video transition: ${finalFileInfo.filename} | Artist: ${finalFileInfo.artist} | Title: ${finalFileInfo.title} | Album: ${finalFileInfo.album}")
                        }
                    }

                    val videoIdMatch = Regex("""watch\?v=([a-zA-Z0-9_-]{11})""").find(line)
                    currentVideoId = videoIdMatch?.groupValues?.get(1)

                    // Reset for new file
                    currentFileInfo = DownloadedFileInfo("", null, null, null, currentVideoId, false)
                    Log.d(TAG, "Processing video: $currentVideoId")
                }

                // Track metadata from yt-dlp output for summary
                // Pattern: [MetadataParser] Parsed artist from '%(uploader)s': 'Kendrick Lamar'
                if (line.contains("[MetadataParser] Parsed artist from")) {
                    val artistMatch = Regex("""Parsed artist from.*?:\s*'([^']+)'""").find(line)
                    artistMatch?.let {
                        // Sanitize artist value - remove any embedded newlines
                        val rawArtist = it.groupValues[1]
                        val artist = rawArtist
                            .replace("\n", " ")
                            .replace("\r", " ")
                            .replace(Regex("\\s+"), " ")
                            .trim()
                        currentFileInfo = currentFileInfo.copy(artist = artist)
                        Log.d(TAG, "Captured artist: '$artist'")
                    }
                }

                // Pattern: [MetadataParser] Parsed meta_title from '%(title)s': 'Money Trees'
                // OR: [MetadataParser] Parsed title from '%(title)s': 'Some Title'
                if (line.contains("[MetadataParser] Parsed meta_title from") ||
                    line.contains("[MetadataParser] Parsed title from")) {
                    val titleMatch = Regex("""Parsed (?:meta_)?title from.*?:\s*'([^']+)'""").find(line)
                    titleMatch?.let {
                        // Sanitize title value - remove any embedded newlines
                        val rawTitle = it.groupValues[1]
                        val title = rawTitle
                            .replace("\n", " ")
                            .replace("\r", " ")
                            .replace(Regex("\\s+"), " ")
                            .trim()
                        currentFileInfo = currentFileInfo.copy(title = title)
                        Log.d(TAG, "Captured title: '$title'")
                    }
                }

                // Pattern: [MetadataParser] Parsed meta_album from '%(album,playlist_title,playlist|)s': 'good kid, m.A.A.d city'
                // OR album could come from video's own metadata
                if (line.contains("[MetadataParser] Parsed meta_album from")) {
                    val albumMatch = Regex("""Parsed meta_album from.*?:\s*'([^']+)'""").find(line)
                    albumMatch?.let {
                        // Sanitize album value - remove any embedded newlines or extra whitespace
                        val rawAlbum = it.groupValues[1]

                        // First, get the first line only (in case yt-dlp outputs multiple lines)
                        val firstLine = rawAlbum.lines().firstOrNull { it.isNotBlank() } ?: rawAlbum

                        // Then clean up any remaining whitespace
                        val album = firstLine
                            .replace("\r", " ")
                            .replace(Regex("\\s+"), " ")
                            .trim()
                            .takeIf { it.isNotBlank() }

                        // Capture the album that yt-dlp actually reported
                        if (album != null) {
                            currentFileInfo = currentFileInfo.copy(album = album)
                            Log.d(TAG, "Captured album from MetadataParser: '$album'")
                        }
                    }
                }

                // Pattern: [MetadataParser] Changed album to: testtest
                // This appears when --replace-in-metadata successfully modifies the album field
                if (line.contains("[MetadataParser] Changed album to:")) {
                    val changedAlbumMatch = Regex("""Changed album to:\s*(.+)""").find(line)
                    changedAlbumMatch?.let {
                        val album = it.groupValues[1].trim()
                        if (album.isNotBlank()) {
                            currentFileInfo = currentFileInfo.copy(album = album)
                            Log.d(TAG, "Captured album from 'Changed album to': '$album'")
                        }
                    }
                }

                // Pattern: [MetadataParser] Setting meta_album from 'literal': 'playlist title'
                // This appears when --parse-metadata successfully sets the album field
                if (line.contains("[MetadataParser]") && line.contains("Setting meta_album from")) {
                    val setAlbumMatch = Regex("""Setting meta_album from.*?:\s*'([^']+)'""").find(line)
                    setAlbumMatch?.let {
                        val album = it.groupValues[1].trim()
                        if (album.isNotBlank()) {
                            currentFileInfo = currentFileInfo.copy(album = album)
                            Log.d(TAG, "Captured album from 'Setting meta_album': '$album'")
                        }
                    }
                }

                // Pattern: [ModifyMetadata] Replace field album: "old value" -> "new value"
                // This pattern detects when --replace-in-metadata actually modifies the album field
                if (line.contains("[ModifyMetadata]") && line.contains("album", ignoreCase = true)) {
                    val albumReplaceMatch = Regex("""album.*?->\s*"([^"]+)"""", RegexOption.IGNORE_CASE).find(line)
                    albumReplaceMatch?.let {
                        val replacedAlbumValue = it.groupValues[1].trim()
                        if (replacedAlbumValue.isNotBlank()) {
                            currentFileInfo = currentFileInfo.copy(album = replacedAlbumValue)
                            Log.d(TAG, "Captured album from ModifyMetadata replacement: '$replacedAlbumValue'")
                        }
                    }
                }

                // Pattern: Check if yt-dlp outputs album in FFmpeg metadata or other post-processor logs
                if (line.contains("album", ignoreCase = true) &&
                    (line.contains("[ExtractAudio]") || line.contains("[FFmpeg]") || line.contains("[Metadata]"))) {
                    // Try to extract album value from various metadata output formats
                    val genericAlbumMatch = Regex("""album[:\s=]+["']?([^"'\n]+)["']?""", RegexOption.IGNORE_CASE).find(line)
                    genericAlbumMatch?.let {
                        val potentialAlbum = it.groupValues[1].trim()
                        // Only update if we don't already have an album and the value looks valid
                        if (currentFileInfo.album == null && potentialAlbum.isNotBlank() && potentialAlbum.length < 100) {
                            currentFileInfo = currentFileInfo.copy(album = potentialAlbum)
                            Log.d(TAG, "Captured album from processor output: '$potentialAlbum'")
                        }
                    }
                }

                // Pattern: [Metadata] Adding metadata to "/path/to/file.opus"
                // This confirms metadata is being written to the file
                if (line.contains("[Metadata] Adding metadata to")) {
                    val pathMatch = Regex("""Adding metadata to "(.+\.opus)"""").find(line)
                    pathMatch?.let {
                        val fullPath = it.groupValues[1]
                        val filename = File(fullPath).name

                        // Check if this video is from fallback
                        val isFallback = currentVideoId?.let { replacementMap.containsKey(it) } ?: false

                        // Only use album that was actually parsed from yt-dlp output
                        // Do NOT apply replacedAlbum here - if yt-dlp doesn't confirm it, don't assume it
                        currentFileInfo = currentFileInfo.copy(
                            filename = filename,
                            videoId = currentVideoId,
                            isFallback = isFallback
                        )
                        Log.d(TAG, "Captured filename: $filename (fallback: $isFallback, album: ${currentFileInfo.album})")
                    }
                }

                // Pattern: [EmbedThumbnail] mutagen: Adding thumbnail
                // This is usually the last step, good place to finalize the current file
                if (line.contains("[EmbedThumbnail]") && line.contains("Adding thumbnail")) {
                    if (currentFileInfo.filename.isNotEmpty()) {
                        // Check if we already added this file (to avoid duplicates)
                        val alreadyAdded = downloadedFiles.any { it.filename == currentFileInfo.filename && it.videoId == currentFileInfo.videoId }

                        if (!alreadyAdded) {
                            // If title is still null, try to extract it from the filename
                            var finalTitle = currentFileInfo.title
                            if (finalTitle == null && currentFileInfo.filename.isNotEmpty()) {
                                // Remove file extension
                                val nameWithoutExt = currentFileInfo.filename.substringBeforeLast(".")

                                // Try to extract title from "Artist - Title" format
                                if (nameWithoutExt.contains(" - ")) {
                                    // Get the part after the first " - "
                                    finalTitle = nameWithoutExt.substringAfter(" - ")
                                    Log.d(TAG, "Extracted title from filename: $finalTitle")
                                } else {
                                    // Use the whole filename as title
                                    finalTitle = nameWithoutExt
                                    Log.d(TAG, "Using full filename as title: $finalTitle")
                                }
                            }

                            // Use injectedAlbum as fallback if album wasn't detected from yt-dlp output
                            val finalAlbum = currentFileInfo.album ?: injectedAlbum

                            val finalFileInfo = currentFileInfo.copy(title = finalTitle, album = finalAlbum)

                            downloadedFiles.add(finalFileInfo)
                            Log.i(TAG, "Completed file: ${finalFileInfo.filename} | Artist: ${finalFileInfo.artist} | Title: ${finalFileInfo.title} | Album: ${finalFileInfo.album} | Fallback: ${finalFileInfo.isFallback}")
                        }

                        // Always reset after thumbnail (whether added or not)
                        currentFileInfo = DownloadedFileInfo("", null, null, null, null, false)
                    }
                }

                runBlocking {
                    eventBus.post(DownloadWorker.WorkerProgress(progress.toInt(), line, itemId.toLong(), logItem.id))
                }

                val title: String = command.take(65)
                notificationUtil.updateTerminalDownloadNotification(
                    itemId,
                    line, progress.toInt(), title,
                    NotificationUtil.DOWNLOAD_SERVICE_CHANNEL_ID
                )
                CoroutineScope(Dispatchers.IO).launch {
                    if (logDownloads) logRepo.update(line, logItem.id)
                    dao.updateLog(line, itemId.toLong())
                }
            }
        }.onSuccess { result ->
            // Capture any remaining file that wasn't finalized during processing
            if (currentFileInfo.filename.isNotEmpty()) {
                // Check if we already added this file (to avoid duplicates)
                val alreadyAdded = downloadedFiles.any { it.filename == currentFileInfo.filename && it.videoId == currentFileInfo.videoId }

                if (!alreadyAdded) {
                    var finalTitle = currentFileInfo.title
                    if (finalTitle == null) {
                        val nameWithoutExt = currentFileInfo.filename.substringBeforeLast(".")
                        finalTitle = if (nameWithoutExt.contains(" - ")) {
                            nameWithoutExt.substringAfter(" - ")
                        } else {
                            nameWithoutExt
                        }
                        Log.d(TAG, "Extracted title from filename for final file: $finalTitle")
                    }

                    // Use injectedAlbum as fallback if album wasn't detected from yt-dlp output
                    val finalAlbum = currentFileInfo.album ?: injectedAlbum

                    downloadedFiles.add(currentFileInfo.copy(title = finalTitle, album = finalAlbum))
                    Log.i(TAG, "Added final file: ${currentFileInfo.filename} | Title: $finalTitle | Album: $finalAlbum")
                }
            }

            // Check if process was killed for fallback
            if (shouldKillProcess && unavailableVideoIds.isNotEmpty()) {
                Log.i(TAG, "Process was killed due to unavailable video(s). Starting fallback immediately...")
                // Don't log as success, proceed directly to fallback
            } else {
                Log.i(TAG, "=== Execution completed normally ===")
            }

            // Check if any unavailable videos were detected during execution
            if (unavailableVideoIds.isNotEmpty()) {
                Log.i(TAG, "Processing ${unavailableVideoIds.size} unavailable video(s): ${unavailableVideoIds.joinToString(", ")}")

                // Attempt fallback for each unavailable video
                val replacements = mutableMapOf<String, String>()
                val commandTemplateDao = DBManager.getInstance(context).commandTemplateDao
                val ytdlpUtil = YTDLPUtil(context, commandTemplateDao)

                unavailableVideoIds.forEach { videoId ->
                    val originalUrl = "https://www.youtube.com/watch?v=$videoId"
                    Log.i(TAG, "Attempting fallback for: $originalUrl")

                    val fallbackResult = runCatching {
                        runBlocking {
                            ytdlpUtil.attemptUnavailableFallback(originalUrl)
                        }
                    }.getOrNull()

                    if (fallbackResult != null) {
                        val (_, replacementUrl) = fallbackResult
                        val replacementId = replacementUrl.substringAfter("watch?v=").substringBefore("&")
                        replacements[videoId] = replacementId
                        Log.i(TAG, "Found replacement: $videoId → $replacementId")
                    } else {
                        Log.w(TAG, "No replacement found for $videoId")
                    }
                }

                if (replacements.isNotEmpty()) {
                    val fallbackMessage = "Found ${replacements.size} replacement(s):\n" +
                        replacements.map { (old, new) -> "  $old → $new" }.joinToString("\n")
                    Log.i(TAG, fallbackMessage)
                    dao.updateLog("$fallbackMessage\n\nRestarting download with replacements...", itemId.toLong())

                    // Build updated command with batch file
                    val updatedCommand: String

                    // Check if we're already using a batch file (from previous retry)
                    val batchFileMatch = Regex("""-a\s+([^\s]+)""").find(command)
                    val playlistMatch = Regex("""(playlist\?list=[^&\s]+)""").find(command)

                    if (batchFileMatch != null) {
                        // Already using batch file - update it directly
                        val batchFilePath = batchFileMatch.groupValues[1]
                        val batchFile = File(batchFilePath)

                        Log.i(TAG, "📝 Updating existing batch file: $batchFilePath")

                        if (batchFile.exists()) {
                            // Read existing URLs
                            val existingUrls = batchFile.readLines().filter { it.isNotBlank() }
                            Log.i(TAG, "📋 Found ${existingUrls.size} URLs in existing batch file")

                            // Update URLs: replace unavailable with replacements
                            val urlList = mutableListOf<String>()
                            var replacedCount = 0
                            var skippedCount = 0

                            existingUrls.forEach { url ->
                                val videoId = url.substringAfter("watch?v=").substringBefore("&").take(11)
                                when {
                                    replacements.containsKey(videoId) -> {
                                        // Replace with new video
                                        val replacementId = replacements[videoId]!!
                                        urlList.add("https://www.youtube.com/watch?v=$replacementId")
                                        replacedCount++
                                        Log.d(TAG, "  ↪️ Replacing $videoId with $replacementId")
                                    }
                                    unavailableVideoIds.contains(videoId) -> {
                                        // Skip unavailable video with no replacement
                                        skippedCount++
                                        Log.d(TAG, "  ⏭️ Skipping unavailable $videoId (no replacement)")
                                    }
                                    else -> {
                                        // Keep original
                                        urlList.add(url)
                                    }
                                }
                            }

                            Log.i(TAG, "✅ Updated batch file: ${urlList.size} videos (replaced: $replacedCount, skipped: $skippedCount)")
                            batchFile.writeText(urlList.joinToString("\n"))
                            updatedCommand = command  // Command stays the same, file is updated
                        } else {
                            Log.w(TAG, "⚠️ Batch file not found, keeping command as-is")
                            updatedCommand = command
                        }
                    } else if (playlistMatch != null) {
                        // First time - fetch playlist and create batch file
                        val playlistUrl = "https://www.youtube.com/${playlistMatch.value}"
                        val baseCommand = command.substringBefore(playlistMatch.value)
                        val afterUrl = command.substringAfter(playlistMatch.value).trim()

                        Log.i(TAG, "📋 Fetching all videos from playlist to rebuild...")

                        // Fetch all video IDs from playlist
                        val allVideoIds = runCatching {
                            runBlocking {
                                val playlistRequest = YoutubeDLRequest(playlistUrl)
                                playlistRequest.addOption("--flat-playlist")
                                playlistRequest.addOption("--print", "id")
                                val response = YoutubeDL.getInstance().execute(playlistRequest)
                                response.out.lines().filter { it.length == 11 && it.matches(Regex("[a-zA-Z0-9_-]{11}")) }
                            }
                        }.getOrElse { emptyList() }

                        if (allVideoIds.isNotEmpty()) {
                            Log.i(TAG, "📊 Found ${allVideoIds.size} total videos in playlist")

                            val urlList = mutableListOf<String>()
                            var replacedCount = 0
                            var skippedCount = 0

                            allVideoIds.forEach { videoId ->
                                when {
                                    replacements.containsKey(videoId) -> {
                                        // Use replacement
                                        val replacementId = replacements[videoId]!!
                                        urlList.add("https://www.youtube.com/watch?v=$replacementId")
                                        replacedCount++
                                        Log.d(TAG, "  ↪️ Replacing $videoId with $replacementId")
                                    }
                                    unavailableVideoIds.contains(videoId) -> {
                                        // Skip unavailable video with no replacement
                                        skippedCount++
                                        Log.d(TAG, "  ⏭️ Skipping unavailable $videoId (no replacement)")
                                    }
                                    else -> {
                                        // Keep original
                                        urlList.add("https://www.youtube.com/watch?v=$videoId")
                                    }
                                }
                            }

                            Log.i(TAG, "✅ Built URL list: ${urlList.size} videos (replaced: $replacedCount, skipped: $skippedCount)")

                            val urlFile = File(context.cacheDir, "playlist_with_replacements_${itemId}.txt")
                            urlFile.writeText(urlList.joinToString("\n"))

                            updatedCommand = "$baseCommand -a ${urlFile.absolutePath} $afterUrl"
                            Log.i(TAG, "📝 Created URL list file: ${urlFile.absolutePath}")
                        } else {
                            // Fallback: couldn't fetch playlist, just add replacements
                            Log.w(TAG, "⚠️ Could not fetch playlist videos, using replacements only")
                            val urlList = replacements.values.map { "https://www.youtube.com/watch?v=$it" }
                            val urlFile = File(context.cacheDir, "playlist_with_replacements_${itemId}.txt")
                            urlFile.writeText(urlList.joinToString("\n"))
                            updatedCommand = "$baseCommand -a ${urlFile.absolutePath} $afterUrl"
                        }
                    } else {
                        // Single video: direct replacement
                        updatedCommand = if (unavailableVideoIds.size == 1) {
                            unavailableVideoIds.firstOrNull()?.let { videoId ->
                                val replacementId = replacements[videoId]
                                if (replacementId != null) {
                                    val originalUrl = "https://www.youtube.com/watch?v=$videoId"
                                    val replacementUrl = "https://www.youtube.com/watch?v=$replacementId"
                                    command.replace(originalUrl, replacementUrl)
                                } else command
                            } ?: command
                        } else command
                    }

                    // Update command in database
                    runBlocking {
                        dao.updateCommand(updatedCommand, itemId.toLong())
                    }

                    handler.postDelayed({
                        val msg = if (replacements.size > 1) {
                            "Found ${replacements.size} alternative videos. Retrying..."
                        } else {
                            "Video unavailable. Found alternative, retrying..."
                        }
                        Toast.makeText(context, msg, Toast.LENGTH_LONG).show()
                    }, 100)

                    notificationUtil.cancelDownloadNotification(itemId)
                    // Return retry immediately - don't proceed with success handling
                    return Result.retry()
                } else {
                    Log.w(TAG, "⚠️ No replacements found for unavailable videos")
                    dao.updateLog("❌ Could not find alternatives for ${unavailableVideoIds.size} unavailable video(s)", itemId.toLong())
                }
            }

            // Original success handling - only reached if no unavailable videos or no replacements found
            if(!noCache){
                //move file from internal to set download directory
                try {
                    Log.i(TAG, "Moving files from cache to: $downloadLocation")
                    FileUtil.moveFile(File(FileUtil.getCachePath(context) + "/TERMINAL/" + itemId),context, downloadLocation!!, false){ p ->
                        eventBus.post(DownloadWorker.WorkerProgress(p, "", itemId.toLong(), logItem.id))
                    }
                    Log.i(TAG, "Successfully moved files to final destination")
                }catch (e: Exception){
                    Log.e(TAG, "Failed to move files: ${e.message}", e)
                    e.printStackTrace()
                    handler.postDelayed({
                        Toast.makeText(context, e.message, Toast.LENGTH_SHORT).show()
                    }, 1000)
                }
            }

            // Generate simplified summary log of downloaded files
            if (downloadedFiles.isNotEmpty() || failedVideos.isNotEmpty()) {
                val summaryLog = buildString {
                    appendLine()
                    val totalItems = downloadedFiles.size + failedVideos.size
                    appendLine("Total files: ${downloadedFiles.size} downloaded, ${failedVideos.size} failed")

                    var index = 1
                    downloadedFiles.forEach { fileInfo ->
                        val status = if (fileInfo.isFallback) {
                            "Original Unavailable (Used Fallback)"
                        } else {
                            "Original"
                        }

                        appendLine("[${index}/${totalItems}] $status")
                        index++
                    }

                    failedVideos.forEach { (_, url) ->
                        appendLine("[${index}/${totalItems}] Unavailable ($url)")
                        index++
                    }

                    appendLine()
                }

                Log.i(TAG, summaryLog)

                // Append summary to terminal log by adding it to result.out
                // This ensures the formatted summary appears in the terminal
                val finalOutput = result.out + "\n" + summaryLog

                if (logDownloads) logRepo.update(initialLogDetails + finalOutput, logItem.id, true)
                dao.updateLog(finalOutput, itemId.toLong())
            } else {
                // No downloaded files, just update with raw output
                if (logDownloads) logRepo.update(initialLogDetails + result.out, logItem.id, true)
                dao.updateLog(result.out, itemId.toLong())
            }

            notificationUtil.cancelDownloadNotification(itemId)
            delay(1000)
            dao.delete(itemId.toLong())

            // Clean up replacement map file
            val replacementMapFile = File(context.cacheDir, "replacements_${itemId}.json")
            if (replacementMapFile.exists()) {
                replacementMapFile.delete()
                Log.d(TAG, "Cleaned up replacement map file")
            }
        }.onFailure {
            val errorMessage = it.message ?: ""
            Log.e(TAG, "Terminal download failed: $errorMessage", it)

            // Check if files were actually downloaded despite the exception
            val cacheDir = File(FileUtil.getCachePath(context) + "/TERMINAL/" + itemId)
            val hasDownloadedFiles = cacheDir.exists() && cacheDir.listFiles()?.isNotEmpty() == true

            // Check if this is an empty exception (yt-dlp completion signal) with successful downloads
            val isEmptyException = errorMessage.isBlank() || errorMessage.trim().isEmpty()

            if (hasDownloadedFiles && (isEmptyException || !noCache)) {
                Log.i(TAG, "✅ Files were downloaded successfully despite exception (empty: $isEmptyException) - proceeding with file move")

                // Move files to final destination
                if (!noCache) {
                    try {
                        Log.i(TAG, "Moving files from cache to: $downloadLocation")
                        FileUtil.moveFile(cacheDir, context, downloadLocation!!, false) { p ->
                            eventBus.post(DownloadWorker.WorkerProgress(p, "", itemId.toLong(), logItem.id))
                        }
                        Log.i(TAG, "Successfully moved files to final destination")
                    } catch (e: Exception) {
                        Log.e(TAG, "Failed to move files: ${e.message}", e)
                        handler.postDelayed({
                            Toast.makeText(context, "Download completed but failed to move files: ${e.message}", Toast.LENGTH_LONG).show()
                        }, 1000)
                    }
                }

                // Generate simplified summary log even in exception case
                if (downloadedFiles.isNotEmpty() || failedVideos.isNotEmpty()) {
                    val summaryLog = buildString {
                        appendLine()
                        val totalItems = downloadedFiles.size + failedVideos.size
                        appendLine("Total files: ${downloadedFiles.size} downloaded, ${failedVideos.size} failed")

                        var index = 1
                        downloadedFiles.forEachIndexed { _, fileInfo ->
                            val isFallback = fileInfo.videoId?.let { replacementMap.containsKey(it) } ?: false
                            val status = if (isFallback) {
                                "Original Unavailable (Used Fallback)"
                            } else {
                                "Original"
                            }

                            appendLine("[${index}/${totalItems}] $status")
                            index++
                        }

                        failedVideos.forEach { (_, url) ->
                            appendLine("[${index}/${totalItems}] Unavailable ($url)")
                            index++
                        }

                        appendLine()
                    }

                    Log.i(TAG, summaryLog)

                    // Append summary to terminal log
                    val currentLog = dao.getTerminalById(itemId.toLong())?.log ?: ""
                    dao.updateLog(currentLog + "\n" + summaryLog, itemId.toLong())
                }

                // Clean up and mark as success
                if (logDownloads) {
                    val terminalItem = runBlocking { dao.getTerminalById(itemId.toLong()) }
                    logRepo.update(initialLogDetails + (terminalItem?.log ?: ""), logItem.id, true)
                }
                // Don't overwrite the log - it already has the summary appended above
                // dao.updateLog("Download completed successfully!", itemId.toLong())
                notificationUtil.cancelDownloadNotification(itemId)
                delay(1000)
                dao.delete(itemId.toLong())

                // Clean up replacement map file
                val replacementMapFile = File(context.cacheDir, "replacements_${itemId}.json")
                if (replacementMapFile.exists()) {
                    replacementMapFile.delete()
                    Log.d(TAG, "Cleaned up replacement map file")
                }

                handler.postDelayed({
                    Toast.makeText(context, "Download completed successfully!", Toast.LENGTH_SHORT).show()
                }, 100)

                return Result.success()
            }

            // Check if we intentionally killed the process for fallback
            if (shouldKillProcess && unavailableVideoIds.isNotEmpty()) {
                Log.i(TAG, "⚠️ Process was intentionally killed for fallback. Reason: $killProcessReason")
                Log.i(TAG, "🔄 Detected ${unavailableVideoIds.size} unavailable video(s): ${unavailableVideoIds.joinToString(", ")}")

                // Run fallback logic
                val replacements = mutableMapOf<String, String>()
                val commandTemplateDao = DBManager.getInstance(context).commandTemplateDao
                val ytdlpUtil = YTDLPUtil(context, commandTemplateDao)

                unavailableVideoIds.forEach { videoId ->
                    val originalUrl = "https://www.youtube.com/watch?v=$videoId"
                    Log.i(TAG, "🔍 Attempting fallback for: $originalUrl")

                    val fallbackResult = runCatching {
                        runBlocking {
                            ytdlpUtil.attemptUnavailableFallback(originalUrl)
                        }
                    }.getOrNull()

                    if (fallbackResult != null) {
                        val (_, replacementUrl) = fallbackResult
                        val replacementId = replacementUrl.substringAfter("watch?v=").substringBefore("&")
                        replacements[videoId] = replacementId
                        Log.i(TAG, "✅ Found replacement: $videoId → $replacementId")
                    } else {
                        Log.w(TAG, "❌ No replacement found for $videoId")
                        failedVideos[videoId] = originalUrl
                    }
                }

                if (replacements.isNotEmpty()) {
                    // Update the replacementMap with new replacements (replacementId -> originalId)
                    replacements.forEach { (originalId, replacementId) ->
                        replacementMap[replacementId] = originalId
                        Log.d(TAG, "Added to replacementMap: $replacementId -> $originalId")
                    }

                    // Save replacement map to file for next retry
                    val replacementMapFile = File(context.cacheDir, "replacements_${itemId}.json")
                    try {
                        val jsonContent = replacementMap.entries.joinToString(",", "{", "}") { (k, v) ->
                            "\"$k\":\"$v\""
                        }
                        replacementMapFile.writeText(jsonContent)
                        Log.d(TAG, "Saved replacement map to file")
                    } catch (e: Exception) {
                        Log.w(TAG, "Failed to save replacement map: ${e.message}")
                    }

                    val fallbackMessage = "🎯 Found ${replacements.size} replacement(s):\n" +
                        replacements.map { (old, new) -> "  $old → $new" }.joinToString("\n")
                    Log.i(TAG, fallbackMessage)
                    dao.updateLog("$fallbackMessage\n\n📥 Restarting download with replacements...", itemId.toLong())

                    // Build updated command
                    val updatedCommand: String

                    // Check if we're already using a batch file (from previous retry)
                    val batchFileMatch = Regex("""-a\s+([^\s]+)""").find(command)
                    val playlistMatch = Regex("""(playlist\?list=[^&\s]+)""").find(command)

                    if (batchFileMatch != null) {
                        // Already using batch file - update it directly
                        val batchFilePath = batchFileMatch.groupValues[1]
                        val batchFile = File(batchFilePath)

                        Log.i(TAG, "📝 Updating existing batch file: $batchFilePath")

                        if (batchFile.exists()) {
                            // Read existing URLs
                            val existingUrls = batchFile.readLines().filter { it.isNotBlank() }
                            Log.i(TAG, "📋 Found ${existingUrls.size} URLs in existing batch file")

                            // Update URLs: replace unavailable with replacements
                            val urlList = mutableListOf<String>()
                            var replacedCount = 0
                            var skippedCount = 0

                            existingUrls.forEach { url ->
                                val videoId = url.substringAfter("watch?v=").substringBefore("&").take(11)
                                when {
                                    replacements.containsKey(videoId) -> {
                                        // Replace with new video
                                        val replacementId = replacements[videoId]!!
                                        urlList.add("https://www.youtube.com/watch?v=$replacementId")
                                        replacedCount++
                                        Log.d(TAG, "  ↪️ Replacing $videoId with $replacementId")
                                    }
                                    unavailableVideoIds.contains(videoId) -> {
                                        // Skip unavailable video with no replacement
                                        skippedCount++
                                        Log.d(TAG, "  ⏭️ Skipping unavailable $videoId (no replacement)")
                                    }
                                    else -> {
                                        // Keep original
                                        urlList.add(url)
                                    }
                                }
                            }

                            Log.i(TAG, "✅ Updated batch file: ${urlList.size} videos (replaced: $replacedCount, skipped: $skippedCount)")
                            batchFile.writeText(urlList.joinToString("\n"))
                            updatedCommand = command  // Command stays the same, file is updated
                        } else {
                            Log.w(TAG, "⚠️ Batch file not found, keeping command as-is")
                            updatedCommand = command
                        }
                    } else if (playlistMatch != null) {
                        // First time - fetch playlist and create batch file
                        val playlistUrl = "https://www.youtube.com/${playlistMatch.value}"
                        val baseCommand = command.substringBefore(playlistMatch.value)
                        val afterUrl = command.substringAfter(playlistMatch.value).trim()

                        Log.i(TAG, "📋 Fetching all videos from playlist to rebuild...")

                        // Fetch all video IDs from playlist
                        val allVideoIds = runCatching {
                            runBlocking {
                                val playlistRequest = YoutubeDLRequest(playlistUrl)
                                playlistRequest.addOption("--flat-playlist")
                                playlistRequest.addOption("--print", "id")
                                val response = YoutubeDL.getInstance().execute(playlistRequest)
                                response.out.lines().filter { it.length == 11 && it.matches(Regex("[a-zA-Z0-9_-]{11}")) }
                            }
                        }.getOrElse { emptyList() }

                        if (allVideoIds.isNotEmpty()) {
                            Log.i(TAG, "📊 Found ${allVideoIds.size} total videos in playlist")

                            val urlList = mutableListOf<String>()
                            var replacedCount = 0
                            var skippedCount = 0

                            allVideoIds.forEach { videoId ->
                                when {
                                    replacements.containsKey(videoId) -> {
                                        // Use replacement
                                        val replacementId = replacements[videoId]!!
                                        urlList.add("https://www.youtube.com/watch?v=$replacementId")
                                        replacedCount++
                                        Log.d(TAG, "  ↪️ Replacing $videoId with $replacementId")
                                    }
                                    unavailableVideoIds.contains(videoId) -> {
                                        // Skip unavailable video with no replacement
                                        skippedCount++
                                        Log.d(TAG, "  ⏭️ Skipping unavailable $videoId (no replacement)")
                                    }
                                    else -> {
                                        // Keep original
                                        urlList.add("https://www.youtube.com/watch?v=$videoId")
                                    }
                                }
                            }

                            Log.i(TAG, "✅ Built URL list: ${urlList.size} videos (replaced: $replacedCount, skipped: $skippedCount)")

                            val urlFile = File(context.cacheDir, "playlist_with_replacements_${itemId}.txt")
                            urlFile.writeText(urlList.joinToString("\n"))

                            updatedCommand = "$baseCommand -a ${urlFile.absolutePath} $afterUrl"
                            Log.i(TAG, "📝 Created URL list file: ${urlFile.absolutePath}")
                        } else {
                            // Fallback: couldn't fetch playlist, just add replacements
                            Log.w(TAG, "⚠️ Could not fetch playlist videos, using replacements only")
                            val urlList = replacements.values.map { "https://www.youtube.com/watch?v=$it" }
                            val urlFile = File(context.cacheDir, "playlist_with_replacements_${itemId}.txt")
                            urlFile.writeText(urlList.joinToString("\n"))
                            updatedCommand = "$baseCommand -a ${urlFile.absolutePath} $afterUrl"
                        }
                    } else {
                        updatedCommand = if (unavailableVideoIds.size == 1) {
                            unavailableVideoIds.firstOrNull()?.let { videoId ->
                                replacements[videoId]?.let { replacementId ->
                                    command.replace(
                                        "https://www.youtube.com/watch?v=$videoId",
                                        "https://www.youtube.com/watch?v=$replacementId"
                                    )
                                } ?: command
                            } ?: command
                        } else command
                    }

                    runBlocking {
                        dao.updateCommand(updatedCommand, itemId.toLong())
                    }

                    handler.postDelayed({
                        val msg = if (replacements.size > 1) {
                            "Found ${replacements.size} alternatives. Retrying..."
                        } else {
                            "Found alternative video. Retrying..."
                        }
                        Toast.makeText(context, msg, Toast.LENGTH_SHORT).show()
                    }, 100)

                    notificationUtil.cancelDownloadNotification(itemId)
                    return Result.retry()
                }
            }

            // Get the terminal log which contains the actual error output
            val terminalItem = runBlocking { dao.getTerminalById(itemId.toLong()) }
            val terminalLog = terminalItem?.log ?: ""

            Log.d(TAG, "Terminal log content (last 500 chars): ${terminalLog.takeLast(500)}")

            // Check for unavailable video errors in the log (not the exception message)
            val isUnavailableError = terminalLog.contains("unavailable", ignoreCase = true) ||
                    terminalLog.contains("Video unavailable", ignoreCase = true) ||
                    terminalLog.contains("not available", ignoreCase = true) ||
                    terminalLog.contains("private video", ignoreCase = true) ||
                    terminalLog.contains("deleted video", ignoreCase = true)

            // Check for format not available errors (different from video unavailable)
            val isFormatError = terminalLog.contains("Requested format is not available", ignoreCase = true) ||
                    terminalLog.contains("format is not available", ignoreCase = true)

            if (isFormatError) {
                Log.w(TAG, "Format availability error detected - video exists but requested format not available")
                dao.updateLog("Note: Some videos have format issues. Try different quality settings.", itemId.toLong())
            }

            if (isUnavailableError) {
                Log.d(TAG, "Detected unavailable video error in terminal command")

                // Extract all YouTube video URLs from the terminal log
                // For playlists, we need to find the specific video that failed
                val videoIdPattern = Regex("""\[youtube\] ([a-zA-Z0-9_-]{11}): (?:Video unavailable|.*unavailable)""")
                val unavailableVideoIds = videoIdPattern.findAll(terminalLog)
                    .map { it.groupValues[1] }
                    .toSet()

                Log.i(TAG, "Found ${unavailableVideoIds.size} unavailable video(s): ${unavailableVideoIds.joinToString(", ")}")

                if (unavailableVideoIds.isNotEmpty()) {
                    // For each unavailable video, attempt to find replacement
                    val replacements = mutableMapOf<String, String>()

                    val commandTemplateDao = DBManager.getInstance(context).commandTemplateDao
                    val ytdlpUtil = YTDLPUtil(context, commandTemplateDao)

                    unavailableVideoIds.forEach { videoId ->
                        val originalUrl = "https://www.youtube.com/watch?v=$videoId"
                        Log.i(TAG, "Attempting fallback for unavailable video: $originalUrl")

                        val fallbackResult = runCatching {
                            runBlocking {
                                ytdlpUtil.attemptUnavailableFallback(originalUrl)
                            }
                        }.getOrNull()

                        if (fallbackResult != null) {
                            val (_, replacementUrl) = fallbackResult
                            // Extract replacement video ID
                            val replacementId = replacementUrl.substringAfter("watch?v=").substringBefore("&")
                            replacements[videoId] = replacementId
                            Log.i(TAG, "Found replacement: $videoId → $replacementId ($replacementUrl)")
                        } else {
                            Log.w(TAG, "No replacement found for $videoId")
                            failedVideos[videoId] = originalUrl
                        }
                    }

                    if (replacements.isNotEmpty()) {
                        // Update the replacementMap with new replacements (replacementId -> originalId)
                        replacements.forEach { (originalId, replacementId) ->
                            replacementMap[replacementId] = originalId
                            Log.d(TAG, "Added to replacementMap: $replacementId -> $originalId")
                        }

                        // Save replacement map to file for next retry
                        val replacementMapFile = File(context.cacheDir, "replacements_${itemId}.json")
                        try {
                            val jsonContent = replacementMap.entries.joinToString(",", "{", "}") { (k, v) ->
                                "\"$k\":\"$v\""
                            }
                            replacementMapFile.writeText(jsonContent)
                            Log.d(TAG, "Saved replacement map to file")
                        } catch (e: Exception) {
                            Log.w(TAG, "Failed to save replacement map: ${e.message}")
                        }

                        val fallbackMessage = "Found replacements for ${replacements.size} unavailable video(s):\n" +
                            replacements.map { (old, new) -> "$old → $new" }.joinToString("\n")
                        Log.i(TAG, fallbackMessage)
                        dao.updateLog("$fallbackMessage\nRestarting download with replacements...", itemId.toLong())

                        // Build the updated command
                        val updatedCommand: String

                        // Check if it's a playlist URL
                        val playlistMatch = Regex("""(playlist\?list=[^&\s]+)""").find(command)
                        if (playlistMatch != null) {
                            // For playlists, we need to create a custom command that includes replacements
                            // Build a new command that downloads both the playlist and the replacement videos
                            val playlistUrl = playlistMatch.value
                            val baseCommand = command.substringBefore(playlistMatch.value)
                            val afterUrl = command.substringAfter(playlistMatch.value).trim()

                            // Create a file list with all video URLs
                            val urlList = mutableListOf<String>()

                            // Add original playlist (will skip unavailable)
                            urlList.add("https://www.youtube.com/$playlistUrl")

                            // Add replacement videos
                            replacements.values.forEach { replacementId ->
                                urlList.add("https://www.youtube.com/watch?v=$replacementId")
                            }

                            // Create a temp file with URLs
                            val urlFile = File(context.cacheDir, "playlist_with_replacements_${itemId}.txt")
                            urlFile.writeText(urlList.joinToString("\n"))

                            // Modify command to use batch file
                            updatedCommand = "$baseCommand -a ${urlFile.absolutePath} $afterUrl"

                            Log.i(TAG, "Created URL list file: ${urlFile.absolutePath}")
                        } else {
                            // Single video - direct replacement
                            updatedCommand = if (unavailableVideoIds.size == 1) {
                                unavailableVideoIds.firstOrNull()?.let { videoId ->
                                    val replacementId = replacements[videoId]
                                    if (replacementId != null) {
                                        val originalUrl = "https://www.youtube.com/watch?v=$videoId"
                                        val replacementUrl = "https://www.youtube.com/watch?v=$replacementId"
                                        command.replace(originalUrl, replacementUrl)
                                    } else command
                                } ?: command
                            } else command
                        }

                        // Update command in database
                        runBlocking {
                            dao.updateCommand(updatedCommand, itemId.toLong())
                        }

                        handler.postDelayed({
                            val msg = if (replacements.size > 1) {
                                "Found ${replacements.size} alternative videos. Retrying..."
                            } else {
                                "Video unavailable. Found alternative source, retrying..."
                            }
                            Toast.makeText(context, msg, Toast.LENGTH_LONG).show()
                        }, 100)

                        // Don't delete the item, let it retry
                        notificationUtil.cancelDownloadNotification(itemId)
                        return Result.retry()
                    } else {
                        Log.w(TAG, "No replacements found for any unavailable videos")
                        dao.updateLog("Could not find alternatives for unavailable videos", itemId.toLong())
                    }
                } else {
                    Log.w(TAG, "Could not extract video IDs from unavailable error")
                }
            }

            // Original error handling
            if (it.message != null){
                if (logDownloads) logRepo.update(it.message!!, logItem.id)
                dao.updateLog(it.message!!, itemId.toLong())
            }
            notificationUtil.cancelDownloadNotification(itemId)
            File(FileUtil.getDefaultCommandPath() + "/" + itemId).deleteRecursively()
            Log.e(TAG, context.getString(R.string.failed_download), it)
            delay(1000)
            dao.delete(itemId.toLong())
            return@doWork Result.failure()

        }

        return Result.success()

    }

    companion object {
        private var itemId : Int = 0
        const val TAG = "DownloadWorker"
    }

}