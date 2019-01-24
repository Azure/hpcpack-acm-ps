
# TODO: need login?
function Add-AcmVm {
  param(
    [Parameter(Mandatory = $true)]
    [object] $vm,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountName,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountRG
  )

  Write-Host "Enable MSI for VM $($vm.Name)"
  if ($vm.Identity -eq $null -or !($vm.Identity.Type -contains "SystemAssigned")) {
    Write-Host "Executing for VM $($vm.Name)"
    Update-AzVM -ResourceGroupName $vm.ResourceGroupName -VM $vm -IdentityType "SystemAssigned"
  }
  else {
    Write-Host "The VM $($vm.Name) already has an System Assigned Identity"
  }

  # Update $vm for new $vm.Identity.PrincipalId
  $vm = Get-AzVM -Name $vm.Name -ResourceGroupName $vm.ResourceGroupName

  Write-Host "Add role 'reader' to VM $($vm.Name)"
  try {
    New-AzRoleAssignment -ObjectId $vm.Identity.PrincipalId -RoleDefinitionName "Reader" -ResourceGroupName $vm.ResourceGroupName
  }
  catch {
    if ($_ -contains 'already exists') {
      Write-Host "The VM $($vm.Name) already has role 'Reader' on resouce group $($vm.ResourceGroupName)"
    }
    else {
      throw
    }
  }

  Write-Host "Add role 'Storage Account Contributor' to VM $($vm.Name)"
  try {
    New-AzRoleAssignment -ObjectId $vm.Identity.PrincipalId -RoleDefinitionName "Storage Account Contributor" -ResourceName $storageAccountName -ResourceType "Microsoft.Storage/storageAccounts" -ResourceGroupName $storageAccountRG
  }
  catch {
    Write-Host $_
    if ($_ -contains 'already exists') {
      Write-Host "The VM $($vm.Name) already has role 'Storage Account Contributor' on storage account $($storageAccountName)"
    }
    else {
      throw
    }
  }

  Write-Host "Install HpcAcmAgent for VM $($vm.Name)"
  # TODO: Do not remove it if it is there.
  try {
    Remove-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Name "HpcAcmAgent" -Force
  }
  catch {
    Write-Host "Caught exception: $($_)"
  }
  if ($vm.OSProfile.LinuxConfiguration) {
    $extesionType = "HpcAcmAgent"
  }
  else {
    # Suppose there're only Linux and Windows
    $extesionType = "HpcAcmAgentWin"
  }
  Set-AzVMExtension -Publisher "Microsoft.HpcPack" -ExtensionType $extesionType -ResourceGroupName $vm.ResourceGroupName -TypeHandlerVersion 1.0 -VMName $vm.Name -Location $vm.Location -Name "HpcAcmAgent"
}

function Remove-AcmVm {
  param(
    [Parameter(Mandatory = $true)]
    [object] $vm,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountName,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountRG
  )

  Write-Host "Uninstall HpcAcmAgent for VM $($vm.Name)"
  try {
    Remove-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Name "HpcAcmAgent" -Force
  }
  catch {
    Write-Host "Caught exception: $($_)"
  }

  Write-Host "Remove role 'Storage Account Contributor' from VM $($vm.Name)"
  try {
    Remove-AzRoleAssignment -ObjectId $vm.Identity.PrincipalId -RoleDefinitionName "Storage Account Contributor" -ResourceName $storageAccountName -ResourceType "Microsoft.Storage/storageAccounts" -ResourceGroupName $storageAccountRG
  }
  catch {
    Write-Host "Caught exception: $($_)"
  }

  Write-Host "Remove role 'reader' from VM $($vm.Name)"
  try {
    Remove-AzRoleAssignment -ObjectId $vm.Identity.PrincipalId -RoleDefinitionName "Reader" -ResourceGroupName $vm.ResourceGroupName
  }
  catch {
    Write-Host "Caught exception: $($_)"
  }

  Write-Host "Disable MSI for VM $($vm.Name)"
  if ($vm.Identity -and $vm.Identity.Type -contains "SystemAssigned") {
    try {
      Update-AzVM -ResourceGroupName $vm.ResourceGroupName -VM $vm -IdentityType "None"
    }
    catch {
      Write-Host "Caught exception: $($_)"
    }
  }
}

function Add-AcmVmScaleSet {
  param(
    [Parameter(Mandatory = $true)]
    [object] $vmss,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountName,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountRG
  )

  Write-Host "Enable MSI for VM Scale Set $($vmss.Name)"
  if ($vmss.Identity -eq $null -or !($vmss.Identity.Type -contains "SystemAssigned")) {
    Write-Host "Executing for VMSS $($vmss.Name)"
    Update-AzVmss -ResourceGroupName $vmss.resourceGroupName -VMScaleSetName $vmss.Name -IdentityType "SystemAssigned"
  }
  else {
    Write-Host "The VMSS $($vmss.Name) already has an System Assigned Identity"
  }

  # Update $vm for new $vm.Identity.PrincipalId
  $vmss = Get-AzVmss -Name $vmss.Name -ResourceGroupName $vmss.ResourceGroupName

  Write-Host "Add role 'reader' to VMSS $($vmss.Name)"
  try {
    New-AzRoleAssignment -ObjectId $vmss.Identity.PrincipalId -RoleDefinitionName "Reader" -ResourceGroupName $vmss.ResourceGroupName
  }
  catch {
    if ($_ -contains 'already exists') {
      Write-Host "The VMSS $($vmss.Name) already has role 'Reader' on resouce group $($vmss.ResourceGroupName)"
    }
    else {
      throw
    }
  }

  Write-Host "Add role 'Storage Account Contributor' to VMSS $($vmss.Name)"
  try {
    New-AzRoleAssignment -ObjectId $vmss.Identity.PrincipalId -RoleDefinitionName "Storage Account Contributor" -ResourceName $storageAccountName -ResourceType "Microsoft.Storage/storageAccounts" -ResourceGroupName $storageAccountRG
  }
  catch {
    if ($_ -contains 'already exists') {
      Write-Host "The VMSS $($vmss.Name) already has role 'Storage Account Contributor' on storage account $($storageAccountName)"
    }
    else {
      throw
    }
  }

  Write-Host "Install HpcAcmAgent for VM Scale Set $($vmss.Name)"
  # TODO: Do not remove it if it is there.
  try {
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name "HpcAcmAgent"
    Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
    Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"
  }
  catch {
    Write-Host "Caught exception: $($_)"
  }

  if ($vmss.VirtualMachineProfile.OsProfile.LinuxConfiguration) {
    $extesionType = "HpcAcmAgent"
  }
  else {
    # Suppose there're only Linux and Windows
    $extesionType = "HpcAcmAgentWin"
  }
  Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name "HpcAcmAgent" -Publisher "Microsoft.HpcPack" -Type $extesionType -TypeHandlerVersion 1.0
  Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
  Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"
}

function Remove-AcmVmScaleSet {
  param(
    [Parameter(Mandatory = $true)]
    [object] $vmss,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountName,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountRG
  )

  Write-Host "Uninstall HpcAcmAgent for VM Scale Set $($vmss.Name)"
  try {
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name "HpcAcmAgent"
    Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
    Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"
  }
  catch {
    Write-Host "Caught exception: $($_)"
  }

  Write-Host "Remove role 'Storage Account Contributor' from VMSS $($vmss.Name)"
  try {
    Remove-AzRoleAssignment -ObjectId $vmss.Identity.PrincipalId -RoleDefinitionName "Storage Account Contributor" -ResourceName $storageAccountName -ResourceType "Microsoft.Storage/storageAccounts" -ResourceGroupName $storageAccountRG
  }
  catch {
    Write-Host "Caught exception: $($_)"
  }

  Write-Host "Remove role 'reader' from VMSS $($vmss.Name)"
  try {
    Remove-AzRoleAssignment -ObjectId $vmss.Identity.PrincipalId -RoleDefinitionName "Reader" -ResourceGroupName $vmss.ResourceGroupName
  }
  catch {
    Write-Host "Caught exception: $($_)"
  }

  Write-Host "Disable MSI for VMSS $($vmss.Name)"
  if ($vmss.Identity -and $vmss.Identity.Type -contains "SystemAssigned") {
    try {
      Update-AzVmss -ResourceGroupName $vmss.resourceGroupName -VMScaleSetName $vmss.Name -IdentityType "None"
    }
    catch {
      Write-Host "Caught exception: $($_)"
    }
  }
}

function Set-AcmClusterTag {
  param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $StorageAccountName,

    [Parameter(Mandatory = $true)]
    [string] $StorageAccountRG
  )

  $rg = Get-AzResourceGroup -Name $ResourceGroup
  $tags = $rg.Tags
  $key = "StorageConfiguration"
  $value = "{ `"AccountName`": `"$($StorageAccountName)`", `"ResourceGroup`":`"$($StorageAccountRG)`" }"
  if ($tags -eq $null) {
    $tags = @{ "$key" = "$value" }
  }
  else {
    $tags[$key] = $value
  }
  Set-AzResourceGroup -Tags $tags -Name $ResourceGroup
}

function Reset-AcmClusterTag {
  param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup
  )

  $rg = Get-AzResourceGroup -Name $ResourceGroup
  $tags = $rg.Tags
  if ($tags) {
    $key = "StorageConfiguration"
    $tags.Remove($key)
    Set-AzResourceGroup -Tags $tags -Name $ResourceGroup
  }
}

function Login {
  $ErrorActionPreference = 'Stop'

  $needLogin = $true
  Try {
    $content = Get-AzContext
    if ($content) {
      $needLogin = ([string]::IsNullOrEmpty($content.Account))
    }
  }
  Catch {
    if ($_ -like "*Login-AzAccount to login*") {
      $needLogin = $true
    }
    else {
      throw
    }
  }

  if ($needLogin) {
    Login-AzAccount
  }
}

function Prepare-AcmAzureCtx {
  param($SubscriptionId)
  Login
  Select-AzSubscription -SubscriptionId $SubscriptionId
}

function ShowProgress {
  param($startTime, $timeout, $activity, $status, $op, $id, $pid)

  $now = Get-Date
  $elapsed = ($now - $startTime).TotalSeconds
  $remains = $timeout - $elapsed
  $percent = $elapsed * 100 / $timeout
  $cmd = "Write-Progress -Activity '$($activity)' -PercentComplete $($percent) -SecondsRemaining $($remains)"
  if ($id) {
    $cmd += " -Id $($id)"
  }
  if ($pid) {
    $cmd += " -ParentId $($pid)"
  }
  if ($status) {
    $cmd += " -Status '$($status)'"
  }
  if ($op) {
    $cmd += " -CurrentOperation '$($op)'"
  }
  Invoke-Expression $cmd
}

function HideProgress {
  param($id)
  Write-Progress -Activity "END" -Completed -Id $id
}

function Wait-AcmJob {
  param($jobs, $startTime, $timeout, $activity, $progId)

  $ids = $jobs.foreach('id')
  while ($true) {
    $elapsed = ($(Get-Date) - $startTime).TotalSeconds
    if ($elapsed -ge $Timeout) {
      break
    }
    # TODO: optimize this
    $doneJobCount = $(Get-Job -Id $ids).where({
      $_.state -eq 'Completed' -or $_.state -eq 'Failed' -or $_.state -eq 'Stopped' }).Count
    if ($doneJobCount -eq $ids.Count) {
      break
    }
    ShowProgress $startTime $timeout $activity -Status "Waiting jobs to finish...." `
      -Op "Completed jobs: $($doneJobCount)/$($jobs.Count)" -Id $progId

    # NOTE: DO NOT simply
    #
    # Receive-Job $jobs
    #
    # because that will implicitly add the output to the return value and thus
    # pollute the caller's return value.

    $output = Receive-Job $jobs
    if ($output) {
      Write-Host $output
    }
    Start-Sleep 1
  }
  $output = Receive-Job $jobs
  if ($output) {
    Write-Host $output
  }
}

function Remove-AcmJob {
  param($ids)

  # Remove-Job somtimes don't return even with "-Force". So do it in another job and forget it.
  Start-ThreadJob -ScriptBlock {
    param($ids)
    Remove-Job -Force -Id $ids
  } -ArgumentList $ids | Out-Null
}

function CollectResult {
  param($names, $jobs)

  $result = @()
  for ($idx = 1; $idx -lt $names.Length; $idx++) {
    $result += [PSCustomObject]@{
      Name = $names[$idx]
      Completed = $jobs[$idx].State -eq 'Completed'
      JobId = $jobs[$idx].Id
    }
  }
  return $result
}

function OutputResult {
  param($result)

  $result |
    Sort-Object -Property Completed, Name |
    Format-Table -Property @{Name = 'VM/VM Scale Set'; Expression = {$_.Name}}, Completed, JobId -Wrap |
    Out-Default

  if ($result.Count -gt 0) {
    $completed = $result.where({ $_.Completed }).Count
    $summary = [PSCustomObject]@{
      Total = $result.Count
      Completed = $completed
      Percent = "$('{0:0.00}' -f ($completed * 100 / $result.Count))%"
    }
    $summary | Format-Table -Property Total, Completed, Percent -Wrap | Out-Default
  }
}

function Add-AcmCluster {
  param(
    [Parameter(Mandatory = $true)]
    [string] $SubscriptionId,

    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $AcmResourceGroup,

    [Parameter(Mandatory = $false)]
    [int] $Timeout = 180,

    [Parameter(Mandatory = $false)]
    [switch] $RetainJobs,

    [Parameter(Mandatory = $false)]
    [switch] $Return
  )

  $startTime = Get-Date
  $activity = 'Adding cluster to ACM service...'

  ShowProgress $startTime $Timeout $activity -Status "Login to Azure..." -id 1
  Prepare-AcmAzureCtx $SubscriptionId | Out-Null

  $jobs = @()
  $names = @($null)
  $acmRg = Get-AzResourceGroup -Name $AcmResourceGroup
  $storageAccount = (Get-AzStorageAccount -ResourceGroupName $acmRg.ResourceGroupName)[0]

  ShowProgress $startTime $Timeout $activity -Status "Starting jobs..." -id 1

  # Configure storage information for the resource group
  Write-Host "Setting storage configuration for resource group $ResourceGroup..."
  $jobs += Start-ThreadJob -ScriptBlock ${function:Set-AcmClusterTag} -ArgumentList $ResourceGroup, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName

  # Register each vm and vm scale set to ACM
  Write-Host "Adding VMs and VM scale sets from resource group $ResourceGroup..."
  $vms = Get-AzVm -ResourceGroupName $ResourceGroup
  $vmssSet = Get-AzVmss -ResourceGroupName $ResourceGroup

  # TODO: Apply -ThrottleLimit of Start-ThreadJob based on total jobs
  foreach ($vm in $vms) {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Add-AcmVm} -ArgumentList $vm, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
    $names += $vm.Name
  }
  foreach ($vmss in $vmssSet) {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Add-AcmVmScaleSet} -ArgumentList $vmss, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
    $names += $vmss.Name
  }

  Wait-AcmJob $jobs $startTime $Timeout $activity -ProgId 1

  if (!$RetainJobs) {
    ShowProgress $startTime $Timeout $activity -Status "Cleaning jobs..." -id 1
    $ids = $jobs.foreach('Id')
    Remove-AcmJob $ids
  }
  HideProgress 1

  $result = CollectResult $names $jobs
  if ($Return) {
    return $result
  }
  OutputResult $result
}

function Remove-AcmCluster {
  param(
    [Parameter(Mandatory = $true)]
    [string] $SubscriptionId,

    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $AcmResourceGroup,

    [Parameter(Mandatory = $false)]
    [int] $Timeout = 180,

    [Parameter(Mandatory = $false)]
    [switch] $RetainJobs,

    [Parameter(Mandatory = $false)]
    [switch] $Return
  )

  $startTime = Get-Date
  $activity = 'Removing cluster from ACM service...'

  ShowProgress $startTime $Timeout $activity -Status "Login to Azure..." -id 1
  Prepare-AcmAzureCtx $SubscriptionId | Out-Null

  $jobs = @()
  $names = @($null)
  $acmRg = Get-AzResourceGroup -Name $AcmResourceGroup
  $storageAccount = (Get-AzStorageAccount -ResourceGroupName $acmRg.ResourceGroupName)[0]

  ShowProgress $startTime $Timeout $activity -Status "Starting jobs..." -id 1

  # Configure storage information for the resource group
  Write-Host "Resetting storage configuration for resource group $ResourceGroup..."
  $jobs += Start-ThreadJob -ScriptBlock ${function:Reset-AcmClusterTag} -ArgumentList $ResourceGroup

  # Register each vm and vm scale set to ACM
  Write-Host "Removing VMs and VM scale sets from resource group $ResourceGroup..."
  $vms = Get-AzVm -ResourceGroupName $ResourceGroup
  $vmssSet = Get-AzVmss -ResourceGroupName $ResourceGroup

  foreach ($vm in $vms) {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Remove-AcmVm} -ArgumentList $vm, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
    $names += $vm.Name
  }
  foreach ($vmss in $vmssSet) {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Remove-AcmVmScaleSet} -ArgumentList $vmss, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
    $names += $vmss.Name
  }

  Wait-AcmJob $jobs $startTime $Timeout $activity -ProgId 1

  if (!$RetainJobs) {
    ShowProgress $startTime $Timeout $activity -Status "Cleaning jobs..." -id 1
    $ids = $jobs.foreach('Id')
    Remove-AcmJob $ids
  }
  HideProgress 1

  $result = CollectResult $names $jobs
  if ($Return) {
    return $result
  }
  OutputResult $result
}

function Wait-AcmDiagnosticJob {
  param($job, $conn, $startTime, $timeout, $activity)

  while ($true) {
    $elapsed = ($(Get-Date) - $startTime).TotalSeconds
    if ($elapsed -ge $Timeout) {
      break
    }
    if (($job.State -eq "Finished") -or ($job.State -eq "Failed") -or ($job.State -eq "Canceled")) {
      break
    }
    $percent = $elapsed * 100 / $timeout
    Write-Progress -PercentComplete $percent -Activity $activity -CurrentOperation "Job State: $($job.State)"
    $job = Get-AcmDiagnosticJob -Id $job.Id -Connection $conn
    Start-Sleep 1
  }
  return $job
}

function Test-AcmCluster {
  param(
    [Parameter(Mandatory = $true)]
    [string] $IssuerUrl,

    [Parameter(Mandatory = $true)]
    [string] $ClientId,

    [Parameter(Mandatory = $true)]
    [string] $ClientSecret,

    [Parameter(Mandatory = $true)]
    [string] $ApiBasePoint,

    # TODO: determin timeout on node number
    [Parameter(Mandatory = $false)]
    [int] $Timeout = 600,

    [Parameter(Mandatory = $false)]
    [switch] $Return
  )

  $startTime = Get-Date
  $activity = "Testing cluster in ACM service..."

  $status = "Connecting to ACM service..."
  Write-Host $status
  ShowProgress $startTime $Timeout $activity -Status $status -id 1
  $conn = Connect-Acm -IssuerUrl $IssuerUrl -ClientId $ClientId -ClientSecret $ClientSecret -ApiBasePoint $ApiBasePoint

  $status = "Getting ACM nodes..."
  Write-Host $status
  ShowProgress $startTime $Timeout $activity -Status $status -id 1
  $nodes = Get-AcmNode -Connection $conn -Count 100000
  $names = $nodes.where({ $_.Health -eq 'OK' -and $_.State -eq 'Online' }).foreach('Name')
  if ($names.Count -gt 0) {
    # First, install necessary tools
    $status = "Installing test prerequisites on nodes..."
    Write-Host $status
    ShowProgress $startTime $Timeout $activity -Status $status -id 1
    $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $names -Category 'Prerequisite' -Name 'Intel MPI Installation'
    Wait-AcmDiagnosticJob $job $conn $startTime $Timeout 'Installing test prerequisites...' | Out-Null

    # Then, do test
    $status = "Performing test on nodes..."
    Write-Host $status
    ShowProgress $startTime $Timeout $activity -Status $status -id 1
    $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $names -Category 'MPI' -Name 'Pingpong'
    Wait-AcmDiagnosticJob $job $conn $startTime $Timeout "Performing test..." | Out-Null

    # Finally, get aggreation result
    $status = "Fetching test aggregation result..."
    Write-Host $status
    ShowProgress $startTime $Timeout $activity -Status $status -id 1
    $testResult = Get-AcmDiagnosticJobAggregationResult -Connection $conn -Id $job.Id
    $testResult = ConvertFrom-JsonNewtonsoft $testResult.ToString()
    $goodNodes = New-Object -TypeName System.Collections.Generic.HashSet[string] -ArgumentList @(,$testResult['GoodNodes'])
    $goodCount = $goodNodes.Count
  }
  else {
    $goodNodes = $null
    $goodCount = 0
  }

  HideProgress 1

  Write-Host "Generating result..."

  $nodes = $nodes.foreach({
    $val = [ordered]@{
      Name = $_.Name
      InTest = $_.Health -eq 'OK' -and $_.State -eq 'Online'
    }
    if ($goodNodes -ne $null) {
      $val['Good'] = $goodNodes.Contains($_.Name)
    }
    else {
      $val['Good'] = $null
    }
    [PSCustomObject]$val
  })

  if ($Return) {
    return $nodes
  }

  $nodes | Sort-Object -Property InTest, Good, Name |
    Format-Table -Property @{Name = 'Node'; Expression = {$_.Name}}, InTest, Good -Wrap | Out-Default

  if ($nodes.Count -gt 0) {
    $summary = [PSCustomObject]@{
      Total = $nodes.Count
      Good = $goodCount
      Percent = "$('{0:0.00}' -f ($goodCount * 100 / $nodes.Count))%"
    }
    $summary | Format-Table -Property Total, Good, Percent -Wrap | Out-Default
  }
}

# TODO: optional param: app name
function Get-AcmAppInfo {
  param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $SubscriptionId
  )

  $ErrorActionPreference = 'Stop'

  Prepare-AcmAzureCtx $SubscriptionId | Out-Null

  $app = $(Get-AzWebApp -ResourceGroupName $ResourceGroup)[0]
  $config = Invoke-AzResourceAction -ApiVersion 2016-08-01 -Action list `
    -ResourceGroupName $app.ResourceGroup `
    -ResourceType Microsoft.Web/sites/config `
    -ResourceName "$($app.Name)/authsettings" -Force
  $auth = $config.properties
  return @{
    'IssuerUrl' = $auth.issuer
    'ClientId' = $auth.clientId
    'ClientSecret' = $auth.clientSecret
    'ApiBasePoint' = "https://$($app.DefaultHostName)/v1"
  }
}

function New-AcmTest {
  param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $AcmResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $SubscriptionId
  )

  Write-Host "Adding cluster to ACM service..."
  Add-AcmCluster -SubscriptionId $SubscriptionId -ResourceGroup $ResourceGroup -AcmResourceGroup $AcmResourceGroup

  Write-Host "Testing cluster in ACM service..."
  $app = Get-AcmAppInfo -SubscriptionId $SubscriptionId -ResourceGroup $AcmResourceGroup
  Test-AcmCluster -IssuerUrl $app['IssuerUrl'] -ClientId $app['ClientId'] -ClientSecret $app['ClientSecret'] `
    -ApiBasePoint $app['ApiBasePoint']
}
