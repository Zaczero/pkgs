$ErrorActionPreference = 'Stop'

$artifacts = @{
    AMD64 = @('x64', 'f46ee2475de4cbe287e6e5f7d43c838796b14e7379cd216bdbb28d391429f9fc')
    ARM64 = @('arm64', '8a7c30165f6e9b054fbbe5ba6048acf23c967fd76955f7a5d66dc519542d3393')
}
$arch, $sha256 = $artifacts[$env:PROCESSOR_ARCHITECTURE]
if ($null -eq $arch) {
    throw "Unsupported Windows architecture: $env:PROCESSOR_ARCHITECTURE"
}

$archive = Join-Path $env:RUNNER_TEMP 'sqlite-tools.zip'
$directory = Join-Path $env:RUNNER_TEMP 'sqlite-tools'
curl.exe --fail --location --retry 5 --output $archive "https://sqlite.org/2026/sqlite-tools-win-$arch-3530400.zip"
if ((Get-FileHash -Algorithm SHA256 $archive).Hash -ne $sha256) {
    throw 'SQLite tools checksum mismatch'
}
Expand-Archive -Path $archive -DestinationPath $directory
if (-not (Test-Path (Join-Path $directory 'sqlite3.exe'))) {
    throw 'sqlite3.exe is missing from the SQLite tools archive'
}
$directory | Out-File -FilePath $env:GITHUB_PATH -Encoding utf8 -Append
