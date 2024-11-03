$web_page = Invoke-WebRequest "https://bgp.he.net/exchange/IIX"
$html = ConvertFrom-Html $web_page

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
$exchangeInfoWrapper = $html.SelectNodes('//div') | Where-Object { $_.HasClass('tabdata') }
$exchangeInfoDiv = $exchangeInfoWrapper.SelectNodes('div')

# Iterate through the divs and find the correct index based on the adjacent div with class 'asleft'
$label_div = $exchangeInfoDiv.SelectNodes('div') | Where-Object { $_.HasClass('asleft') }
$value_div = $exchangeInfoDiv.SelectNodes('div') | Where-Object { $_.HasClass('asright') }
$exchangeInfo_obj = [PSCustomObject]@{}

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
$exchangeInfo_obj