
$rir_adoption_url = "https://www.nro.net/wp-content/uploads/rpki-uploads/rir-adoption.csv"
$economy_adoption_url = "https://www.nro.net/wp-content/uploads/rpki-uploads/economy-adoption.csv"
$path = "C:\Users\pawwy\OneDrive\Documents\PS Code\RIR"
$dt = Get-Date
$dt_str = $dt.ToString("dd_MM_yy_hh_mm_ss")
$rir_adoption_path = $path + "/" + $dt_str + "_rir_adoption.csv"
$economy_adoption_path = $path + "/" + $dt_str + "_economy_adoption.csv"
Invoke-WebRequest $rir_adoption_url -OutFile $rir_adoption_path
Invoke-WebRequest $economy_adoption_url -OutFile $economy_adoption_path

$nro_extended_url = "https://ftp.ripe.net/pub/stats/ripencc/nro-stats/latest/nro-delegated-stats"
function ExportFile($data) {

    $dt = Get-Date
    $dt_str = $dt.ToString("dd_MM_yy_hh_mm_ss")
    
    $newFileName = $dt_str + "_" + $data[1] + ".csv"
    $path = "C:\Users\pawwy\OneDrive\Documents\PS Code\RIR\" + $newFileName

    $data[0] | Export-Csv -Path $path -NoTypeInformation
}

$dt = Get-Date
$dt_str = $dt.ToString("dd_MM_yy_hh_mm_ss")
$path = "C:\Users\pawwy\OneDrive\Documents\PS Code\RIR\" 
$nro_extended_path = $path + "/" + $dt_str + "_nro_extended.csv"
Invoke-WebRequest $nro_extended_url -OutFile $nro_extended_path
$content = Get-Content -Path $nro_extended_path
$count = 0
$headerRecord = @()

$summaryRecords = @()

$records = @()
$lineCount = 0
foreach ($line in $content) {
    Write-Progress -Activity "Process" `
        -PercentComplete (($lineCount * 100) / $content.Count) `
        -Status "$(([math]::Round((($lineCount)/$content.Count * 100),0))) %"
    $line_fields = $line.split("|")
    # if($lineCount -eq 50){
    #     break
    # }    
    if ($line_fields[0].StartsWith("#")) {        
        continue;
    }
    if ($lineCount -eq 0) {
        $headerRecord += [PSCustomObject]@{
            version   = $line_fields[0]
            registry  = $line_fields[1]
            serial    = $line_fields[2]
            records   = $line_fields[3]
            startdate = $line_fields[4]
            enddate   = $line_fields[5]
            UTCoffset = $line_fields[6]
        }
    }
    else {        
        if ($line_fields.Count -gt 6) {     
            $records += [PSCustomObject]@{
                registry = $line_fields[0]
                cc       = $line_fields[1]
                type     = $line_fields[2]
                start    = $line_fields[3]
                value    = $line_fields[4]
                date     = $line_fields[5]
                status   = $line_fields[6]
                opaque   = $line_fields[7]
            }

        }
        else {
            $summaryRecords += [PSCustomObject]@{                
                registry = $line_fields[0]
                type     = $line_fields[2]
                count    = $line_fields[3]                
            }
        }
    }
    $lineCount += 1
}
ExportFile($summaryRecords, "Summary")
ExportFile($records, "Records")
ExportFile($headerRecord, "Header")


