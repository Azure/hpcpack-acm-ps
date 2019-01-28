# TODO: need login?
# NOTE: This function is called as a script block by Start-ThreadJob AND THUS
# CAN NOT CALL ANY OTHER INTERNAL FUNCTIONS DIRECTLY. This may need to be FIXED.
function Add-AcmVm {
  param(
    [Parameter(Mandatory = $true)]
    [object] $vm,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountName,

    [Parameter(Mandatory = $true)]
    [string] $storageAccountRG,

    # NOTE: [switch] type can't be passed in an argument list, which is required by Start-ThreadJob.
    [bool] $useExistingAgent = $false
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
  $hasExistingAgent = $false
  if ($useExistingAgent) {
    $extensions = $vm.Extensions
    if ($extensions) {
      for ($i = 0; $i -lt $extensions.Count; $i++) {
        if ($extensions[$i].Id -like '*/extensions/HpcAcmAgent') {
          $hasExistingAgent = $true
          break
        }
      }
    }
    Write-Host "VM $($vm.Name) has existing agent: $($hasExistingAgent)"
  }
  else {
    Write-Host "Try to remove existing agent from VM $($vm.Name)"
    try {
      Remove-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Name "HpcAcmAgent" -Force
    }
    catch {}
  }

  if (!$hasExistingAgent) {
    if ($vm.OSProfile.LinuxConfiguration) {
      $extesionType = "HpcAcmAgent"
    }
    else {
      # Suppose there're only Linux and Windows
      $extesionType = "HpcAcmAgentWin"
    }
    Set-AzVMExtension -Publisher "Microsoft.HpcPack" -ExtensionType $extesionType -ResourceGroupName $vm.ResourceGroupName `
      -TypeHandlerVersion 1.0 -VMName $vm.Name -Location $vm.Location -Name "HpcAcmAgent"
  }
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
    [string] $storageAccountRG,

    # NOTE: [switch] type can't be passed in an argument list, which is required by Start-ThreadJob.
    [bool] $useExistingAgent = $false
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
  $hasExistingAgent = $false
  if ($useExistingAgent) {
    $extensions = $vmss.VirtualMachineProfile.ExtensionProfile.Extensions
    if ($extensions) {
      for ($i = 0; $i -lt $extensions.Count; $i++) {
        if ($extensions[$i].Name -eq 'HpcAcmAgent') {
          $hasExistingAgent = $true
          break
        }
      }
    }
    Write-Host "VM scale set $($vmss.Name) has existing agent: $($hasExistingAgent)"
  }
  else {
    Write-Host "Try to remove existing agent from VM scale set $($vmss.Name)"
    try {
      Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name "HpcAcmAgent"
      Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
      Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"
    }
    catch {}
  }

  if (!$hasExistingAgent) {
    if ($vmss.VirtualMachineProfile.OsProfile.LinuxConfiguration) {
      $extesionType = "HpcAcmAgent"
    }
    else {
      # Suppose there're only Linux and Windows
      $extesionType = "HpcAcmAgentWin"
    }
    Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name "HpcAcmAgent" -Publisher "Microsoft.HpcPack" `
      -Type $extesionType -TypeHandlerVersion 1.0
    Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
    Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"
  }
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
  $args = @{
    Activity = $activity
    PercentComplete = $elapsed * 100 / $timeout
    SecondsRemaining = $timeout - $elapsed
  }
  if ($id) {
    $args['Id'] = $id
  }
  if ($pid) {
    $args['ParentId'] = $pid
  }
  if ($status) {
    $args['Status'] = $status
  }
  if ($op) {
    $args['CurrentOperation'] = $op
  }
  Write-Progress @args
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
    # TODO: Optimize counting?
    $doneJobCount = $(Get-Job -Id $ids).where({ $_.state -in 'Completed', 'Failed', 'Stopped' }).Count
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
    Stop-Job -Id $ids
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

function Initialize-AcmCluster {
  param(
    [Parameter(Mandatory = $true)]
    [string] $SubscriptionId,

    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $AcmResourceGroup,

    [int] $Timeout,

    # NOTE: Do not change the default value and do not provide a bigger one,
    # as Start-ThreadJob won't accept a value > 50 and will raise an error.
    [int] $ConcurrentLimit = 50,

    [switch] $RetainJobs,

    [switch] $Return,

    [switch] $UseExistingAgent,

    [switch] $Uninitialize
  )

  $startTime = Get-Date
  if ($Uninitialize) {
    $activity = 'Removing cluster from ACM service...'
  }
  else {
    $activity = 'Adding cluster to ACM service...'
  }

  $basetime = 360 # Max time to add one VM/VM scale set
  if (!$Timeout) {
    # timelimit will be recomputed later based on number of vms
    $timelimit = $basetime
  }
  else {
    $timelimit = $Timeout
  }

  ShowProgress $startTime $timelimit $activity -Status "Login to Azure..." -id 1
  Prepare-AcmAzureCtx $SubscriptionId | Out-Null

  ShowProgress $startTime $timelimit $activity -Status "Preparing for jobs..." -id 1

  $jobs = @()
  $names = @($null)
  $acmRg = Get-AzResourceGroup -Name $AcmResourceGroup
  $storageAccount = (Get-AzStorageAccount -ResourceGroupName $acmRg.ResourceGroupName)[0]
  # TODO: Filter out only running vms, but what about VM scale set?
  $vms = Get-AzVm -ResourceGroupName $ResourceGroup
  $vmssSet = Get-AzVmss -ResourceGroupName $ResourceGroup

  if (!$Timeout) {
    $total = $vms.Count + $vmssSet.Count
    $timelimit = $basetime * ([math]::Truncate($total / $ConcurrentLimit))
    if (($total % $ConcurrentLimit) -gt 0) {
      $timelimit += $basetime
    }
  }

  ShowProgress $startTime $timelimit $activity -Status "Starting jobs..." -id 1

  # Configure storage information for the resource group
  if ($Uninitialize) {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Reset-AcmClusterTag} -ArgumentList $ResourceGroup
  }
  else {
    $jobs += Start-ThreadJob -ScriptBlock ${function:Set-AcmClusterTag} `
      -ArgumentList $ResourceGroup, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
  }

  # Register each vm and vm scale set to ACM
  foreach ($vm in $vms) {
    $args = $vm, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
    if ($Uninitialize) {
      $func = ${function:Remove-AcmVm}
    }
    else {
      $func = ${function:Add-AcmVm}
      $args += $UseExistingAgent
    }
    $jobs += Start-ThreadJob -ThrottleLimit $ConcurrentLimit -ScriptBlock $func -ArgumentList $args
    $names += $vm.Name
  }
  foreach ($vmss in $vmssSet) {
    $args = $vmss, $storageAccount.StorageAccountName, $storageAccount.ResourceGroupName
    if ($Uninitialize) {
      $func = ${function:Remove-AcmVmScaleSet}
    }
    else {
      $func = ${function:Add-AcmVmScaleSet}
      $args += $UseExistingAgent
    }
    $jobs += Start-ThreadJob -ThrottleLimit $ConcurrentLimit -ScriptBlock $func -ArgumentList $args
    $names += $vmss.Name
  }

  Wait-AcmJob $jobs $startTime $timelimit $activity -ProgId 1

  if (!$RetainJobs) {
    ShowProgress $startTime $timelimit $activity -Status "Cleaning jobs..." -id 1
    $ids = $jobs.foreach('Id')
    Remove-AcmJob $ids
  }
  HideProgress 1

  $result = CollectResult $names $jobs
  OutputResult $result
  if ($Return) {
    return $result
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

    [int] $Timeout,

    [switch] $RetainJobs,

    [switch] $Return,

    [switch] $UseExistingAgent
  )
  Initialize-AcmCluster @PSBoundParameters
}

function Remove-AcmCluster {
  param(
    [Parameter(Mandatory = $true)]
    [string] $SubscriptionId,

    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $AcmResourceGroup,

    [int] $Timeout,

    [switch] $RetainJobs,

    [switch] $Return
  )
  Initialize-AcmCluster @PSBoundParameters -Uninitialize
}

function Wait-AcmDiagnosticJob {
  param($job, $conn, $startTime, $timeout, $activity, $status, $progId)

  $finished = $false
  while ($true) {
    $elapsed = ($(Get-Date) - $startTime).TotalSeconds
    if ($elapsed -ge $Timeout) {
      break
    }
    if (($job.State -eq "Finished") -or ($job.State -eq "Failed") -or ($job.State -eq "Canceled")) {
      $finished = $true
      break
    }
    $op = "Diagnostic job state: $($job.State)"
    ShowProgress $startTime $timeout $activity $status $op $progId
    $job = Get-AcmDiagnosticJob -Id $job.Id -Connection $conn
    Start-Sleep 1
  }
  return $finished
}

function Test-AcmCluster {
  param(
    [Parameter(Mandatory = $true)]
    [string] $ApiBasePoint,

    [string] $IssuerUrl,

    [string] $ClientId,

    [string] $ClientSecret,

    [int] $Timeout,

    [switch] $Return
  )

  $startTime = Get-Date
  $activity = "Testing cluster in ACM service..."

  # The meanings of basetime and basesize are:
  # every basesize number of nodes requires basetime to run
  $basetime = 600
  $basesize = 80

  if (!$Timeout) {
    # timelimit will be recomputed later based on number of test nodes
    $timelimit = $basetime
  }
  else {
    $timelimit = $Timeout
  }

  $status = "Connecting to ACM service..."
  Write-Host $status
  ShowProgress $startTime $timelimit $activity -Status $status -id 1

  $args = @{
    ApiBasePoint = $ApiBasePoint
  }
  # Allow unauthenticated access if the ACM service allows.
  if (![string]::IsNullOrEmpty($IssuerUrl)) {
    $args['IssuerUrl'] = $IssuerUrl
    $args['ClientId'] = $ClientId
    $args['ClientSecret'] = $ClientSecret
  }
  $conn = Connect-Acm @args

  $status = "Getting ACM nodes..."
  Write-Host $status
  ShowProgress $startTime $timelimit $activity -Status $status -id 1

  $nodes = Get-AcmNode -Connection $conn -Count 100000
  $names = $nodes.where({ $_.Health -eq 'OK' -and $_.State -eq 'Online' }).foreach('Name')
  if ($names.Count -gt 0) {
    if (!$Timeout) {
      # Recompute timelimit based on node number.
      $timelimit = [Math]::Truncate($nodes.Count / $basesize) * $basetime
      if ($nodes.Count % $basesize -gt 0) {
        $timelimit += $basetime
      }
    }

    # First, install necessary tools
    $status = "Installing test prerequisites on nodes..."
    Write-Host $status
    ShowProgress $startTime $timelimit $activity -Status $status -id 1

    $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $names -Category 'Prerequisite' -Name 'Intel MPI Installation'
    $finished = Wait-AcmDiagnosticJob $job $conn $startTime $timelimit $activity $status -progId 1
    if (!$finished) {
      throw "Prerequisite installation timed out. Job id: $($job.id)"
    }

    # Then, do test
    $status = "Performing test on nodes..."
    Write-Host $status
    ShowProgress $startTime $timelimit $activity -Status $status -id 1

    $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $names -Category 'MPI' -Name 'Pingpong'
    $finished = Wait-AcmDiagnosticJob $job $conn $startTime $timelimit $activity $status -progId 1
    if (!$finished) {
      throw "Test job timed out. Job id: $($job.id)"
    }

    # Finally, get aggreation result
    $status = "Fetching test aggregation result..."
    Write-Host $status
    ShowProgress $startTime $timelimit $activity -Status $status -id 1

    $testResult = Get-AcmDiagnosticJobAggregationResult -Connection $conn -Id $job.Id
    $testResult = ConvertFrom-JsonNewtonsoft $testResult.ToString()

    # NOTE: Conversion to [string[]] is required, otherwise creating object will fail as
    # it can't find a proper constructor for HashSet type.
    $goodNodes = New-Object -TypeName System.Collections.Generic.HashSet[string] `
      -ArgumentList @(, ($testResult['GoodNodes'] -as [string[]]))
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

  $nodes | Sort-Object -Property InTest, Good, Name |
    Format-Table -Wrap -Property `
      @{Name = 'Node'; Expression = {$_.Name}}, `
      @{Name = 'Good for Test'; Expression = {$_.InTest}}, `
      @{Name = 'Good in MPI Pingpong'; Expression = {$_.Good}} | Out-Default

  if ($nodes.Count -gt 0) {
    $summary = [PSCustomObject]@{
      Total = $nodes.Count
      Good = $goodCount
      Percent = "$('{0:0.00}' -f ($goodCount * 100 / $nodes.Count))%"
    }
    $summary | Format-Table -Property Total, Good, Percent -Wrap | Out-Default
  }

  if ($Return) {
    return $nodes
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
    [string] $SubscriptionId,

    [switch] $UseExistingAgent,

    [switch] $NoSetup,

    [int] $SetupTimeout,

    [int] $TestTimeout
  )

  if (!$NoSetup) {
    Write-Host "Adding cluster to ACM service..."
    $args = @{
      SubscriptionId = $SubscriptionId
      ResourceGroup = $ResourceGroup
      AcmResourceGroup = $AcmResourceGroup
      UseExistingAgent = $UseExistingAgent
    }
    if ($SetupTimeout) {
      $args['Timeout'] = $SetupTimeout
    }
    Add-AcmCluster @args
  }

  Write-Host "Getting ACM service app configuration..."
  $app = Get-AcmAppInfo -SubscriptionId $SubscriptionId -ResourceGroup $AcmResourceGroup
  if (!$app['IssuerUrl']) {
    Write-Warning "No authentication configuration is found for the ACM app in $($AcmResourceGroup)!"
  }

  Write-Host "Testing cluster in ACM service..."
  if ($TestTimeout) {
    $app['Timeout'] = $TestTimeout
  }
  Test-AcmCluster @app
}
