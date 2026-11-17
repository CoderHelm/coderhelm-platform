# d. `d_echoed_markers`

**What happened:** The echoed buildspec holds the upload markers, so every build read as "could not be uploaded".

**The guarantee:** A marker counts only on a line of its own. The exit code is the last one printed. A failed generator's reason quotes what the generator printed, not the echoed buildspec.
