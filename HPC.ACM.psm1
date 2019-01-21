
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

  $ErrorActionPreference = 'Stop'

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
    $_
  }
  Set-AzVMExtension -Publisher "Microsoft.HpcPack" -ExtensionType "HpcAcmAgent" -ResourceGroupName $vm.ResourceGroupName -TypeHandlerVersion 1.0 -VMName $vm.Name -Location $vm.Location -Name "HpcAcmAgent"
  Write-Host "OK"
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
    $_
  }

  Write-Host "Remove role 'Storage Account Contributor' from VM $($vm.Name)"
  try {
    Remove-AzRoleAssignment -ObjectId $vm.Identity.PrincipalId -RoleDefinitionName "Storage Account Contributor" -ResourceName $storageAccountName -ResourceType "Microsoft.Storage/storageAccounts" -ResourceGroupName $storageAccountRG
  }
  catch {
    $_
  }

  Write-Host "Remove role 'reader' from VM $($vm.Name)"
  try {
    Remove-AzRoleAssignment -ObjectId $vm.Identity.PrincipalId -RoleDefinitionName "Reader" -ResourceGroupName $vm.ResourceGroupName
  }
  catch {
    $_
  }

  Write-Host "Disable MSI for VM $($vm.Name)"
  if ($vm.Identity -and $vm.Identity.Type -contains "SystemAssigned") {
    try {
      Update-AzVM -ResourceGroupName $vm.ResourceGroupName -VM $vm -IdentityType "None"
    }
    catch {
      $_
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

  $ErrorActionPreference = 'Stop'

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
    $_
  }

  Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name "HpcAcmAgent" -Publisher "Microsoft.HpcPack" -Type "HpcAcmAgent" -TypeHandlerVersion 1.0
  Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
  Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"

  Write-Host "OK"
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
    $_
  }

  Write-Host "Remove role 'Storage Account Contributor' from VMSS $($vmss.Name)"
  try {
    Remove-AzRoleAssignment -ObjectId $vmss.Identity.PrincipalId -RoleDefinitionName "Storage Account Contributor" -ResourceName $storageAccountName -ResourceType "Microsoft.Storage/storageAccounts" -ResourceGroupName $storageAccountRG
  }
  catch {
    $_
  }

  Write-Host "Remove role 'reader' from VMSS $($vmss.Name)"
  try {
    Remove-AzRoleAssignment -ObjectId $vmss.Identity.PrincipalId -RoleDefinitionName "Reader" -ResourceGroupName $vmss.ResourceGroupName
  }
  catch {
    $_
  }

  Write-Host "Disable MSI for VMSS $($vmss.Name)"
  if ($vmss.Identity -and $vmss.Identity.Type -contains "SystemAssigned") {
    try {
      Update-AzVmss -ResourceGroupName $vmss.resourceGroupName -VMScaleSetName $vmss.Name -IdentityType "None"
    }
    catch {
      $_
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

  $ErrorActionPreference = 'Stop'

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

function WaitJob {
  param($jobs, $startTime, $timeout)

  $ids = $jobs.foreach('id')
  while ($true) {
    $elapsed = ($(Get-Date) - $startTime).TotalSeconds
    if ($elapsed -ge $Timeout) {
      break
    }
    # TODO: optimize this
    $runningJobCount = $(Get-Job -Id $ids).where({ $_.state -eq 'Running'}).Count
    if ($runningJobCount -eq 0) {
      break
    }
    $percent = $elapsed * 100 / $timeout
    $doneJobCount = $jobs.Count - $runningJobCount
    Write-Progress -PercentComplete $percent -Activity "Waiting for jobs to complete..." `
      -CurrentOperation "Complete jobs: $($doneJobCount)/$($jobs.Count)"
    Receive-Job $jobs
    Start-Sleep 1
  }
  Receive-Job $jobs
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
    [int] $Timeout = 300
  )

  $startTime = Get-Date
  Login
  Select-AzSubscription -SubscriptionId $SubscriptionId

  $jobs = @()
  $acmRg = Get-AzResourceGroup -Name $AcmResourceGroup
  $storageAccount = (Get-AzStorageAccount -ResourceGroupName $acmRg.ResourceGroupName)[0]

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
  }
  foreach ($vmss in $vmssSet) {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Add-AcmVmScaleSet} -ArgumentList $vmss, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
  }

  WaitJob $jobs $startTime $Timeout

  # Remove-Job somtimes don't return even with -Force
  # Remove-Job -Force -Job $jobs

  $jobs
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
    [int] $Timeout = 300
  )

  $startTime = Get-Date
  Login
  Select-AzSubscription -SubscriptionId $SubscriptionId

  $jobs = @()
  $acmRg = Get-AzResourceGroup -Name $AcmResourceGroup
  $storageAccount = (Get-AzStorageAccount -ResourceGroupName $acmRg.ResourceGroupName)[0]

  # Configure storage information for the resource group
  Write-Host "Resetting storage configuration for resource group $ResourceGroup..."
  $jobs += Start-ThreadJob -ScriptBlock ${function:Reset-AcmClusterTag} -ArgumentList $ResourceGroup

  # Register each vm and vm scale set to ACM
  Write-Host "Removing VMs and VM scale sets from resource group $ResourceGroup..."
  $vms = Get-AzVm -ResourceGroupName $ResourceGroup
  $vmssSet = Get-AzVmss -ResourceGroupName $ResourceGroup

  foreach ($vm in $vms) {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Remove-AcmVm} -ArgumentList $vm, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
  }
  foreach ($vmss in $vmssSet) {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Remove-AcmVmScaleSet} -ArgumentList $vmss, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
  }

  WaitJob $jobs $startTime $Timeout
  Remove-Job -Force -Job $jobs
  $jobs
}

function Wait-AcmDiagnosticJob {
  param($job, $conn)

  $percent = 0
  while (($job.State -ne "Finished") -and ($job.State -ne "Failed") -and ($job.State -ne "Canceled")) {
    Write-Progress -PercentComplete (++$percent % 100) -Activity "Waiting for job to complete..." -CurrentOperation "Job State: $($job.State)"
    Start-Sleep 1
    $job = Get-AcmDiagnosticJob -Id $job.Id -Connection $conn
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
    [string] $ApiBasePoint
  )

  $ErrorActionPreference = 'Stop'

  Write-Host "Connecting to Acm..."
  $conn = Connect-Acm -IssuerUrl $IssuerUrl -ClientId $ClientId -ClientSecret $ClientSecret -ApiBasePoint $ApiBasePoint

  Write-Host "Getting Acm nodes..."
  $nodes = Get-AcmNode -Connection $conn -Count 100000
  $names = $nodes.ForEach('Name')

  # First, install necessary tools
  # TODO: make test cat and name variables with default value
  Write-Host "Installing test prerequisites on nodes..."
  $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $names -Category 'Prerequisite' -Name 'Intel MPI Installation'
  Wait-AcmDiagnosticJob $job $conn

  # Then, do test
  # TODO: make test cat and name variables with default value
  Write-Host "Performing test on nodes..."
  $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $names -Category 'MPI' -Name 'Pingpong'
  Wait-AcmDiagnosticJob $job $conn

  # Finally, get aggreation result
  Write-Host "Getting test report..."
  $result = Get-AcmDiagnosticJobAggregationResult -Connection $conn -Id $job.Id
  return ConvertFrom-JsonNewtonsoft $result.ToString()
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
  Login
  Select-AzSubscription -SubscriptionId $SubscriptionId
  $app = $(Get-AzWebApp -ResourceGroupName $ResourceGroup)[0]
  $config = Invoke-AzResourceAction -ApiVersion 2016-08-01 -Action list -ResourceGroupName $app.ResourceGroup -ResourceType Microsoft.Web/sites/config -ResourceName "$($app.Name)/authsettings" -Force
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

  $ErrorActionPreference = 'Stop'
  Write-Host "Adding cluster to ACM..."
  Add-AcmCluster -SubscriptionId $SubscriptionId -ResourceGroup $ResourceGroup -AcmResourceGroup $AcmResourceGroup
  Write-Host "Getting ACM app info..."
  $app = Get-AcmAppInfo -SubscriptionId $SubscriptionId -ResourceGroup $AcmResourceGroup
  Write-Host "Testing cluster..."
  Test-AcmCluster -IssuerUrl $app['IssuerUrl'] -ClientId $app['ClientId'] -ClientSecret $app['ClientSecret'] -ApiBasePoint $app['ApiBasePoint']
}
