$headers = @{
    "content-type" = "application/json"
}
$baseURL = "https://api.ixpdb.net/v1"
function hitHTTPRequest($relativeURL) {
    try {
        $url = $baseURL + $relativeURL
        $response = Invoke-RestMethod $url -Method 'GET' -Headers $headers
        return $response
    }
    catch {
        Write-Output $relativeURL
    }
}

function ExportFile($data) {

    $dt = Get-Date
    $dt_str = $dt.ToString("dd_MM_yy_hh_mm_ss")
    
    $newFileName = $dt_str + "_" + $data[1] + ".csv"
    $path = "C:\Users\pawwy\OneDrive\Documents\PS Code\CSV Export\" + $newFileName

    $data[0] | Export-Csv -Path $path -NoTypeInformation
}

$providerRecords = hitHTTPRequest("/provider/list")
$organisationRecords = @()

$orgID = $providerRecords | Select-Object organization_id
$orgUniqID = $orgID | Select-Object -Unique organization_id
$organisation_contact = @()
foreach ($id in $orgUniqID) {
    $url = "/organization/" + $id.organization_id  
    $orgres = hitHTTPRequest($url)
    $organisationRecords += [PSCustomObject]@{
        name        = $orgres.name
        id          = $orgres.id
        website     = $orgres.website
        city        = $orgres.city
        country     = $orgres.country
        association = $orgres.association -join ", "        
    }
    foreach ($contact in $orgres.contacts) {
        $organisation_contact += [PSCustomObject]@{
            organization_id = $orgres.id
            phone           = $contact.phone
            name            = $contact.name
            email           = $contact.email
            address         = $contact.address
            city            = $contact.city
            country         = $contact.country
        }
    }

}

$trafficRecords = hitHTTPRequest("/traffic/list")

$participantRecords = @()
$participantResponse = hitHTTPRequest("/participant/list")

$organisationProivderMappingRecords = @()
foreach ($orgRecord in $organisationRecords) {
    $provider_mappingRecords = hitHTTPRequest("/organization/" + $orgRecord.id + "/providers")
    foreach ($map in $provider_mappingRecords) {
        $organisationProivderMappingRecords += [PSCustomObject]@{
            ProviderID     = $map.id
            OrganizationID = $orgRecord.id
        }
    }
}

foreach ($participantRecord in $participantResponse) {
    $participantRecords += [PSCustomObject]@{
        asn            = $participantRecord.asn
        ip_addresses   = $participantRecord.ip_addresses -join ", "
        ipv6           = $participantRecord.ipv6
        manrs          = $participantRecord.manrs
        name           = $participantRecord.name
        provider_count = $participantRecord.provider_count    
    }
}

$provider_networks = @()
$provider_participants = @()
$providerRecordsV2 = @()
foreach ($provider in $providerRecords) {    
    $networks = hitHTTPRequest("/provider/" + $provider.id + "/networks")
    foreach ($network in $networks) {
        $provider_networks += [PSCustomObject]@{
            ProviderID         = $provider.id
            name               = $network.name
            addresses          = $network.addresses -join ", "
            router_server_asns = $network.router_server_asns
        }
    }
    $participants = hitHTTPRequest("/provider/" + $provider.id + "/participants")
    foreach ($participant in $participants) {
        $provider_participants += [PSCustomObject]@{
            ProviderID   = $provider.id
            asn          = $participant.asn
            name         = $participant.name
            ipv6         = $participant.ipv6
            ip_addresses = $participant.ip_addresses -join ", "
        }
    } 
    $providerRecordsV2 += [PSCustomObject]@{
        apis_ixfexport    = $provider.apis.ixfexport
        apis_traffic      = $provider.apis.traffic
        city              = $provider.city
        country           = $provider.country
        id                = $provider.id
        location_count    = $provider.location_count
        looking_glass     = $provider.looking_glass -join ", "
        manrs             = $provider.manrs
        name              = $provider.name
        organization_id   = $provider.organization_id
        participant_count = $provider.participant_count
        pdb_id            = $provider.pdb_id
        updated           = $provider.updated
        website           = $provider.website


    }
}

ExportFile($providerRecordsV2, "Providers")
ExportFile($provider_participants, "Providers_Participants")
ExportFile($provider_networks, "Providers_Networks")
ExportFile($organisationRecords, "Organizations")
ExportFile($organisationProivderMappingRecords, "Organizations_Providers")
ExportFile($trafficRecords, "Traffic")
ExportFile($organisation_contact, "Contacts")