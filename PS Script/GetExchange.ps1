#scraping_single_book_info_with_PowerHTML.ps1
function ExportFile($data) {

    $dt = Get-Date
    $dt_str = $dt.ToString("dd_MM_yy_hh_mm_ss")
    
    $newFileName = $dt_str + "_" + $data[1] + ".csv"
    $path = "C:\Users\pawwy\OneDrive\Documents\PS Code\Exchange Export\" + $newFileName

    $data[0] | Export-Csv -Path $path -NoTypeInformation
}

function Get-TrimmedString {
    param (
        [string]$inputString
    )

    if ([string]::IsNullOrWhiteSpace($inputString)) {
        return ""
    }
    else {
        return  [System.Web.HttpUtility]::HtmlDecode($inputString.Trim())
    }
}

$baseURL = "https://bgp.he.net"

function Extract_Exchange() {

    $web_page = Invoke-WebRequest 'https://bgp.he.net/report/exchanges'
    $html = ConvertFrom-Html $web_page

    $exchangeTable = $html.SelectNodes('//table')[0]
    $exchangeTableBody = $exchangeTable.SelectNodes('tbody')
    $count = 1
    $exchangeRows = @()
    foreach ($row in $exchangeTableBody.SelectNodes('tr')) {
        # if ($count -eq 10) {
        #     break
        # }
        $internetExchange = Get-TrimmedString -inputString $row.SelectSingleNode('td[1]').InnerText
        $internetExchangeURL = $baseURL + (Get-TrimmedString -inputString $row.SelectSingleNode('td[1]').SelectNodes('a').attributes.value)
        $members = Get-TrimmedString -inputString $row.SelectSingleNode('td[2]').InnerText
        $isDataAvailable = Get-TrimmedString -inputString $row.SelectSingleNode('td[3]').SelectNodes('span').InnerText
        $cc = Get-TrimmedString -inputString $row.SelectSingleNode('td[4]').InnerText
        $city = Get-TrimmedString -inputString $row.SelectSingleNode('td[5]').InnerText
        $website = Get-TrimmedString -inputString $row.SelectSingleNode('td[6]').InnerText

        $exchangeRows += [PSCustomObject]@{
            index            = $count
            internetExchange = $internetExchange
            members          = $members
            url              = $internetExchangeURL
            isDataAvailable  = $isDataAvailable
            cc               = $cc
            city             = $city
            website          = $website
        }
        $count += 1
    }
    ExportFile($exchangeRows, "Exchange Data")
    $exchangeTable = $html.SelectNodes('//table')[1]
    $exchangeTableBody = $exchangeTable.SelectNodes('tbody')
    $count = 1
    $participantRows = @()
    foreach ($row in $exchangeTableBody.SelectNodes('tr')) {
        $participantRows += [PSCustomObject]@{
            index    = $count
            asn      = Get-TrimmedString -inputString $row.SelectSingleNode('td[1]').InnerText
            asn_url  = $baseURL + (Get-TrimmedString -inputString $row.SelectSingleNode('td[1]').SelectNodes('a').attributes.value)
            name     = Get-TrimmedString -inputString $row.SelectSingleNode('td[2]').InnerText
            name_url = $baseURL + (Get-TrimmedString -inputString $row.SelectSingleNode('td[2]').SelectNodes('a').attributes.value)
            ixes     = Get-TrimmedString -inputString $row.SelectSingleNode('td[3]').InnerText
        }
    }
    ExportFile($participantRows, "Participant Data")

    $count = 0
    $exchangeInfoHeader = @()
    $exchangeInfoLines = @()
    $lineCount = 0
    foreach ($exchange in $exchangeRows) {
        try {
    
            $web_page = Invoke-WebRequest $exchange.url
            $html = ConvertFrom-Html $web_page
    
            $exchangeInfoWrapper = $html.SelectNodes('//div') | Where-Object { $_.HasClass('tabdata') }
            $exchangeName = $exchangeInfoWrapper.SelectNodes('h2').SelectNodes('a').innertext
            $exchangeInfoDiv = $exchangeInfoWrapper.SelectNodes('div')

            # Iterate through the divs and find the correct index based on the adjacent div with class 'asleft'
            $label_div = $exchangeInfoDiv.SelectNodes('div') | Where-Object { $_.HasClass('asleft') }
            $value_div = $exchangeInfoDiv.SelectNodes('div') | Where-Object { $_.HasClass('asright') }
            $exchangeInfo_obj = [PSCustomObject]@{}
            $exchangeInfo_obj | Add-Member -MemberType NoteProperty -Name "index" -Value $count
            $exchangeInfo_obj | Add-Member -MemberType NoteProperty -Name "Exchange Name" -Value $exchangeName
            $exchangeInfo_obj | Add-Member -MemberType NoteProperty -Name "Exchange URL" -Value $exchange.url
            for ($i = 0; $i -lt $label_div.Count; $i++) {    
                $key = $label_div[$i].innertext.replace(":", "")    
                if ($key -eq "Policy Contact" -or $key -eq "Technical Contact" ) {
                    $strCode = Get-TrimmedString -inputString $value_div[$i].SelectNodes('script').innertext
                    $policyContactEmailCode = [System.Web.HttpUtility]::UrlDecode($strCode.replace("eval(decodeURIComponent('", "").replace("'))", ""))

                    if ($policyContactEmailCode -match "mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})") {
                        $policyContactEmail = $matches[1]    
                    }
                    else {
                        $policyContactEmail = ""
                    }
                    $policyContactPhone = Get-TrimmedString -inputString $value_div[$i].InnerText.Trim().replace("|", "").Trim()
                    $ekey = $key + "_email"
                    $pkey = $key + "_phone"
                    $exchangeInfo_obj | Add-Member -MemberType NoteProperty -Name $ekey -Value $policyContactEmail
                    $exchangeInfo_obj | Add-Member -MemberType NoteProperty -Name $pkey -Value $policyContactPhone
                }
                elseif ($key -eq "Data Feed Health") {        
                    if ($value_div[$i].SelectNodes('img').attributes.Count -gt 1 ) {
                        $dataFeedHealth = Get-TrimmedString -inputString $value_div[$i].SelectNodes('img').attributes[0].value
                    }
                    else {
                        $dataFeedHealth = Get-TrimmedString -inputString $value_div[$i].SelectNodes('img').attributes.value
                    }
                    $exchangeInfo_obj | Add-Member -MemberType NoteProperty -Name $key -Value $dataFeedHealth
                }   
                else {
                    $value = Get-TrimmedString -inputString $value_div[$i].innertext
                    $exchangeInfo_obj | Add-Member -MemberType NoteProperty -Name $key -Value $value  
                }
    
                # $exchangeInfo[$key] = $value_div[$i]    
            }
            $exchangeInfoHeader += $exchangeInfo_obj
            $exchangeInfoTable = $exchangeInfoWrapper.SelectNodes('table')
            $exchangeInfoTBody = $exchangeInfoTable.SelectNodes('tbody')
            $exchangeInfoRows = $exchangeInfoTBody.SelectNodes('tr')
           
            foreach ($tr in $exchangeInfoRows) {
                $exchangeInfoLines += [PSCustomObject]@{
                    index        = $lineCount
                    parentIndex  = $count
                    exchangeName = $exchangeName
                    asn          = Get-TrimmedString -inputString $tr.SelectSingleNode('td[1]').InnerText
                    asn_url      = $baseURL + ( Get-TrimmedString -inputString $tr.SelectSingleNode('td[1]').SelectNodes('a').attributes.value)
                    name         = Get-TrimmedString -inputString $tr.SelectSingleNode('td[2]').InnerText
                    ipv4         = Get-TrimmedString -inputString $tr.SelectSingleNode('td[3]').InnerText
                    ipv6         = Get-TrimmedString -inputString $tr.SelectSingleNode('td[4]').InnerText
                }
                $lineCount += 1
            }
            $count += 1
        }
    
        catch {
            Write-Debug $exchange
        }
    }

    ExportFile($exchangeInfoHeader, "Exchange Header")
    ExportFile($exchangeInfoLines, "Exchange Lines")
}

function Extract_Country() {
    $web_page = Invoke-WebRequest 'https://bgp.he.net/report/world'
    $html = ConvertFrom-Html $web_page

    $countryTable = $html.SelectNodes('//table')[0]
    $countryTableBody = $countryTable.SelectNodes('tbody')
    $count = 1
    $countryRows = @()
    foreach ($row in $countryTableBody.SelectNodes('tr')) {
        # if ($count -eq 10) {
        #     break
        # }
        if ($row.SelectSingleNode('td[1]').SelectNodes('div') -eq 2) {
            $name = Get-TrimmedString -inputString $row.SelectSingleNode('td[1]').SelectNodes('div')[0].SelectNodes('img').attributes[0].value
            $countryFlagURL = $baseURL + (Get-TrimmedString -inputString $row.SelectSingleNode('td[1]').SelectNodes('div')[0].SelectNodes('img').attributes[1].value)
        }
        elseif ($row.SelectSingleNode('td[1]').SelectNodes('div') -eq 1) {
            $name = Get-TrimmedString -inputString $row.SelectSingleNode('td[1]').SelectNodes('div').InnerText
            $countryFlagURL = ""
        }

        $countryRows += [PSCustomObject]@{
            index     = $count            
            name      = $name
            flag_url  = $countryFlagURL
            cc        = Get-TrimmedString -inputString $row.SelectSingleNode('td[2]').InnerText
            asn_count = Get-TrimmedString -inputString $row.SelectSingleNode('td[3]').InnerText
            url       = $baseURL + (Get-TrimmedString -inputString $row.SelectSingleNode('td[4]').SelectNodes('a').attributes.value)
        } 
        $count += 1
    }
    
    ExportFile( $countryRows, "Countries")
    $lineCount = 0
    $countryLines = @()
    foreach ($country in $countryRows) {
        $web_page = Invoke-WebRequest $country.url
        $html = ConvertFrom-Html $web_page

        $countryInfoWrapper = $html.SelectNodes('//div') | Where-Object { $_.HasClass('tabdata') }
        $countryInfoTable = $countryInfoWrapper.SelectNodes('table')
        if ($null -eq $countryInfoTable) {

        }
        else {
            $countryInBody = $countryInfoTable.SelectNodes('tbody')
            $countryInfoRows = $countryInBody.SelectNodes('tr')

            foreach ($tr in $countryInfoRows) {
                $countryLines += [PSCustomObject]@{
                    cc             = $country.cc
                    parentIndex    = $country.index
                    index          = $lineCount
                    asn            = Get-TrimmedString -inputString $tr.SelectSingleNode('td[1]').InnerText
                    asn_url        = $baseURL + ( Get-TrimmedString -inputString $tr.SelectSingleNode('td[1]').SelectNodes('a').attributes.value)
                    name           = Get-TrimmedString -inputString $tr.SelectSingleNode('td[2]').InnerText
                    adjacencies_v4 = Get-TrimmedString -inputString $tr.SelectSingleNode('td[3]').InnerText
                    routes_v4      = Get-TrimmedString -inputString $tr.SelectSingleNode('td[4]').InnerText                
                    adjacencies_v6 = Get-TrimmedString -inputString $tr.SelectSingleNode('td[5]').InnerText
                    routes_v6      = Get-TrimmedString -inputString $tr.SelectSingleNode('td[6]').InnerText

                }
                $lineCount += 1
            }
        }
    }
    ExportFile( $countryLines, "Country Lines")
}

Extract_Country

Extract_Exchange