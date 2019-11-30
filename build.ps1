# Clean directory
Remove-Item -Path "./dist/*" -Recurse

# Create executable
pyinstaller main.spec

# Create settings, and log directories
New-Item -Path "./dist" -Name "settings" -ItemType "directory"
New-Item -Path "./dist" -Name "log" -ItemType "directory"

# Copy stuff over
Copy-Item -Path "./settings/tickers.xlsx" -Destination "./dist/settings/"
Copy-Item -Path "./include/*" -Recurse -Destination "./dist/"

$default = @'
{
    "TWS_ip" : "localhost",
    "TWS_port" : 7497,
    "TWS_id" : 0,
    "TWS_account" : "",
    "timezone" : "US/Eastern",
    "max_portfolio_size" : 13, 
    "round_quantities_to" : 25,
    "primary_sell_type" : "MIDPRICE",
    "auxiliary_sell_type" : "MKT",
    "primary_buy_type" : "MIDPRICE",
    "auxiliary_buy_type" : "",
    "sell_wait_duration" : "",
    "sell_wait_until" : "16:00",
    "buy_wait_duration" : "",
    "buy_wait_until" : ""
}
'@
New-Item -Path "./dist/settings" -Name "settings.json" -Value $default

# Make zip file
Compress-Archive -Path "./dist/*" -CompressionLevel "optimal" -DestinationPath "./dist/AutoBroker"

# Clean directory
Remove-Item -Path "./dist/*" -Exclude "*.zip" -Recurse