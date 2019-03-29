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

    [string] $linuxExtensionUrl,
    [string] $windowsExtensionUrl
  )

  $devMode = $linuxExtensionUrl -or $windowsExtensionUrl

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

  $extName = 'HpcAcmAgent'
  $devExtName = 'HpcAcmAgentDev'
  $knownExts = @($extName, $devExtName)
  $foundExts = @()
  foreach ($ext in $vm.Extensions) {
    foreach ($name in $knownExts) {
      if ($ext.Name -eq $name) {
        $foundExts += $name
      }
    }
  }
  Write-Host "Found $($foundExts.Count) VM Extensions for VM $($vm.Name): $($foundExts -join ', ')"

  foreach ($ext in $foundExts) {
    Write-Host "Remove Extension $ext from VM $($vm.Name)"
    if ($ext -eq $devExtName) {
      if ($vm.OSProfile.LinuxConfiguration) {
        $settings = @{
          timestamp = $(Get-Date).Ticks
          fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/uninstall.sh')
          commandToExecute = "./uninstall.sh"
          skipDos2Unix = $true
        }
        Set-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Location $vm.Location -ExtensionType CustomScript -Publisher Microsoft.Azure.Extensions -TypeHandlerVersion 2.0 -Name $ext -Settings $settings
      }
      else {
        $settings = @{
          timestamp = $(Get-Date).Ticks
          fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/uninstall.ps1')
          commandToExecute = "powershell -ExecutionPolicy Unrestricted -File uninstall.ps1"
        }
        Set-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Location $vm.Location -ExtensionType CustomScriptExtension -Publisher Microsoft.Compute -TypeHandlerVersion 1.9 -Name $ext -Settings $settings
      }
    }
    else {
      Remove-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Name $ext -Force
    }
  }

  if ($devMode) {
    $ext = $devExtName
  }
  else {
    $ext = $extName
  }
  Write-Host "Install $ext for VM $($vm.Name)"

  if ($devMode) {
    if ($vm.OSProfile.LinuxConfiguration) {
      $settings = @{
        timestamp = $(Get-Date).Ticks
        fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/install.sh')
        commandToExecute = "./install.sh $linuxExtensionUrl"
        skipDos2Unix = $true
      }
      $result = Set-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Location $vm.Location -ExtensionType CustomScript -Publisher Microsoft.Azure.Extensions -TypeHandlerVersion 2.0 -Name $ext -Settings $settings
    }
    else {
      $settings = @{
        timestamp = $(Get-Date).Ticks
        fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/install.ps1')
        commandToExecute = "powershell -ExecutionPolicy Unrestricted -File install.ps1 $windowsExtensionUrl"
      }
      $result = Set-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Location $vm.Location -ExtensionType CustomScriptExtension -Publisher Microsoft.Compute -TypeHandlerVersion 1.9 -Name $ext -Settings $settings
    }
  }
  else {
    if ($vm.OSProfile.LinuxConfiguration) {
      $extesionType = "HpcAcmAgent"
    }
    else {
      # Suppose there're only Linux and Windows
      $extesionType = "HpcAcmAgentWin"
    }
    $result = Set-AzVMExtension -Publisher "Microsoft.HpcPack" -ExtensionType $extesionType -ResourceGroupName $vm.ResourceGroupName `
      -TypeHandlerVersion 1.0 -VMName $vm.Name -Location $vm.Location -Name $ext
  }
  if (!$result.IsSuccessStatusCode) {
    throw "Failed installing $ext for VM $($vm.Name)."
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

  $extName = 'HpcAcmAgent'
  $devExtName = 'HpcAcmAgentDev'
  $knownExts = @($extName, $devExtName)
  $foundExts = @()
  foreach ($ext in $vm.Extensions) {
    foreach ($name in $knownExts) {
      if ($ext.Name -eq $name) {
        $foundExts += $name
      }
    }
  }
  Write-Host "Found $($foundExts.Count) VM Extensions for VM $($vm.Name): $($foundExts -join ', ')"

  foreach ($ext in $foundExts) {
    Write-Host "Remove Extension $ext from VM $($vm.Name)"
    if ($ext -eq $devExtName) {
      if ($vm.OSProfile.LinuxConfiguration) {
        $settings = @{
          timestamp = $(Get-Date).Ticks
          fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/uninstall.sh')
          commandToExecute = "./uninstall.sh"
          skipDos2Unix = $true
        }
        Set-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Location $vm.Location -ExtensionType CustomScript -Publisher Microsoft.Azure.Extensions -TypeHandlerVersion 2.0 -Name $ext -Settings $settings
      }
      else {
        $settings = @{
          timestamp = $(Get-Date).Ticks
          fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/uninstall.ps1')
          commandToExecute = "powershell -ExecutionPolicy Unrestricted -File uninstall.ps1"
        }
        Set-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Location $vm.Location -ExtensionType CustomScriptExtension -Publisher Microsoft.Compute -TypeHandlerVersion 1.9 -Name $ext -Settings $settings
      }
    }
    Remove-AzVMExtension -ResourceGroupName $vm.ResourceGroupName -VMName $vm.Name -Name $ext -Force
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

    [string] $linuxExtensionUrl,
    [string] $windowsExtensionUrl
  )

  $devMode = $linuxExtensionUrl -or $windowsExtensionUrl

  Write-Host "Enable MSI for VM Scale Set $($vmss.Name)"
  if ($vmss.Identity -eq $null -or !($vmss.Identity.Type -contains "SystemAssigned")) {
    Write-Host "Executing for VMSS $($vmss.Name)"
    Update-AzVmss -ResourceGroupName $vmss.resourceGroupName -VMScaleSetName $vmss.Name -IdentityType "SystemAssigned"
  }
  else {
    Write-Host "The VMSS $($vmss.Name) already has an System Assigned Identity"
  }

  # Update $vmss for new $vmss.Identity.PrincipalId
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

  $extName = 'HpcAcmAgent'
  $devExtName = 'HpcAcmAgentDev'
  $knownExts = @($extName, $devExtName)
  $foundExts = @()
  foreach ($ext in $vmss.VirtualMachineProfile.ExtensionProfile.Extensions) {
    foreach ($name in $knownExts) {
      if ($ext.Name -eq $name) {
        $foundExts += $name
      }
    }
  }
  Write-Host "Found $($foundExts.Count) VM Extensions for VM scale set $($vmss.Name): $($foundExts -join ', ')"

  foreach ($ext in $foundExts) {
    Write-Host "Remove Extension $ext from VM scale set $($vmss.Name)"

    # Remove-AzVmssExtension simply removes the element of extension by name from
    # array of $vmss.VirtualMachineProfile.ExtensionProfile.Extensions, while
    # Update-AzVmss does the effective work. Remember: only one custom script
    # extension is allowed on one VM/VM scale set.
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext

    if ($ext -eq $devExtName) {
      if ($vmss.VirtualMachineProfile.OsProfile.LinuxConfiguration) {
        $settings = @{
          timestamp = $(Get-Date).Ticks
          fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/uninstall.sh')
          commandToExecute = "./uninstall.sh"
          skipDos2Unix = $true
        }
        Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext -Setting $settings `
          -Type CustomScript -Publisher Microsoft.Azure.Extensions -TypeHandlerVersion 2.0
      }
      else {
        $settings = @{
          timestamp = $(Get-Date).Ticks
          fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/uninstall.ps1')
          commandToExecute = "powershell -ExecutionPolicy Unrestricted -File uninstall.ps1"
        }
        Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext -Setting $settings `
          -Type CustomScriptExtension -Publisher Microsoft.Compute -TypeHandlerVersion 1.9
      }
    }
  }
  Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
  if ($vmss.UpgradePolicy.Mode -eq 'Manual') {
    Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"
  }

  if ($devMode) {
    $ext = $devExtName
  }
  else {
    $ext = $extName
  }
  Write-Host "Install $ext for VM Scale Set $($vmss.Name)"

  if ($devMode) {
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext
    if ($vmss.VirtualMachineProfile.OsProfile.LinuxConfiguration) {
      $settings = @{
        timestamp = $(Get-Date).Ticks
        fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/install.sh')
        commandToExecute = "./install.sh $linuxExtensionUrl"
        skipDos2Unix = $true
      }
      Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext -Setting $settings `
        -Type CustomScript -Publisher Microsoft.Azure.Extensions -TypeHandlerVersion 2.0
    }
    else {
      $settings = @{
        timestamp = $(Get-Date).Ticks
        fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/install.ps1')
        commandToExecute = "powershell -ExecutionPolicy Unrestricted -File install.ps1 $windowsExtensionUrl"
      }
      Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext -Setting $settings `
        -Type CustomScriptExtension -Publisher Microsoft.Compute -TypeHandlerVersion 1.9
    }
  }
  else {
    if ($vmss.VirtualMachineProfile.OsProfile.LinuxConfiguration) {
      $extesionType = "HpcAcmAgent"
    }
    else {
      # Suppose there're only Linux and Windows
      $extesionType = "HpcAcmAgentWin"
    }
    Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext -Publisher "Microsoft.HpcPack" `
      -Type $extesionType -TypeHandlerVersion 1.0
  }
  Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
  if ($vmss.UpgradePolicy.Mode -eq 'Manual') {
    $result = Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"
    if ($result.Status -ne 'Succeeded') {
      throw "Failed installing $ext for VM scale set $($vmss.Name)."
    }
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

  $extName = 'HpcAcmAgent'
  $devExtName = 'HpcAcmAgentDev'
  $knownExts = @($extName, $devExtName)
  $foundExts = @()
  foreach ($ext in $vmss.VirtualMachineProfile.ExtensionProfile.Extensions) {
    foreach ($name in $knownExts) {
      if ($ext.Name -eq $name) {
        $foundExts += $name
      }
    }
  }
  Write-Host "Found $($foundExts.Count) VM Extensions for VM scale set $($vmss.Name): $($foundExts -join ', ')"

  foreach ($ext in $foundExts) {
    Write-Host "Remove Extension $ext from VM scale set $($vmss.Name)"
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext
    if ($ext -eq $devExtName) {
      if ($vmss.VirtualMachineProfile.OsProfile.LinuxConfiguration) {
        $settings = @{
          timestamp = $(Get-Date).Ticks
          fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/uninstall.sh')
          commandToExecute = "./uninstall.sh"
          skipDos2Unix = $true
        }
        Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext -Setting $settings `
          -Type CustomScript -Publisher Microsoft.Azure.Extensions -TypeHandlerVersion 2.0
      }
      else {
        $settings = @{
          timestamp = $(Get-Date).Ticks
          fileUris = @('https://raw.githubusercontent.com/coin8086/node-manager-deployer/master/uninstall.ps1')
          commandToExecute = "powershell -ExecutionPolicy Unrestricted -File uninstall.ps1"
        }
        Add-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $ext -Setting $settings `
          -Type CustomScriptExtension -Publisher Microsoft.Compute -TypeHandlerVersion 1.9
      }
    }
  }
  Update-AzVmss -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -VirtualMachineScaleSet $vmss
  if ($vmss.UpgradePolicy.Mode -eq 'Manual') {
    Update-AzVmssInstance -ResourceGroupName $vmss.ResourceGroupName -VMScaleSetName $vmss.Name -InstanceId "*"
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

function Log {
  param(
    [Parameter(Mandatory = $true)]
    $activity,

    $status,
    $op
  )
  $now = Get-Date
  $ts = $now.ToString('yyyy-MM-dd HH:mm:ss')
  $msg = "[$($ts)][$($activity)]"
  if ($status) {
    $msg += "[$($status)]"
  }
  if ($op) {
    $msg += "[$($op)]"
  }
  Write-Host $msg
}

function ShowProgress {
  param(
    [Parameter(Mandatory = $true)]
    $startTime,

    [Parameter(Mandatory = $true)]
    $timeout,

    [Parameter(Mandatory = $true)]
    $activity,

    $status,
    $op,
    $id,
    $pid,

    [switch] $nolog
  )

  $now = Get-Date
  $elapsed = ($now - $startTime).TotalSeconds
  $percent = $elapsed * 100 / $timeout
  if ($percent -gt 100) {
    $percent = 100
  }
  $args = @{
    Activity = $activity
    PercentComplete = $percent
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

  if (!$nolog) {
    Log $activity $status $op
  }
}

function HideProgress {
  param($id)
  Write-Progress -Activity "END" -Completed -Id $id
}

function Wait-AcmJob {
  param($jobs, $startTime, $timeout, $activity, $progId)

  $status = "Waiting jobs to finish..."
  $pargs = @{
    startTime = $startTime
    timeout = $timeout
    activity = $activity
    status = $status
    id = $progId
  }
  $ids = $jobs.foreach('id')
  $preCount = $null

  while ($true) {
    # TODO: Optimize counting?
    $doneJobCount = $(Get-Job -Id $ids).where({ $_.state -in 'Completed', 'Failed', 'Stopped' }).Count
    $pargs['op'] = "Completed jobs: $($doneJobCount)/$($jobs.Count)"

    if ($doneJobCount -eq $ids.Count) {
      ShowProgress @pargs
      break
    }

    $elapsed = ($(Get-Date) - $startTime).TotalSeconds
    if ($elapsed -ge $Timeout) {
      ShowProgress @pargs
      Log $activity $status 'Timed out!'
      break
    }

    $output = Receive-Job $jobs
    if ($output) {
      ShowProgress @pargs
      Write-Host $output
    }
    else {
      if ($preCount -ne $doneJobCount) {
        ShowProgress @pargs
      }
      else {
        ShowProgress @pargs -nolog
      }
    }
    $preCount = $doneJobCount
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
      JobState = $jobs[$idx].State
      JobId = $jobs[$idx].Id
    }
  }
  return $result
}

function OutputResult {
  param($result)

  $result |
    Sort-Object -Property JobState, Name |
    Format-Table -Property @{Name = 'VM/VM Scale Set'; Expression = {$_.Name}}, JobState, JobId -Wrap |
    Out-Default

  if ($result.Count -gt 0) {
    $completed = $result.where({ $_.JobState -eq 'Completed' }).Count
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

    [switch] $Uninitialize,

    [string] $LinuxExtensionUrl,

    [string] $WindowsExtensionUrl
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
      $args += @($LinuxExtensionUrl, $WindowsExtensionUrl)
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
      $args += @($LinuxExtensionUrl, $WindowsExtensionUrl)
    }
    $jobs += Start-ThreadJob -ThrottleLimit $ConcurrentLimit -ScriptBlock $func -ArgumentList $args
    $names += $vmss.Name
  }

  Wait-AcmJob $jobs $startTime $timelimit $activity -ProgId 1

  if (!$Uninitialize) {
    # Wait for some time for ACM agents to register itself to ACM
    $timeLeft = ($(Get-Date) - $startTime).TotalSeconds
    if (!$Timeout -or $timeLeft -lt $timelimit) {
      $time = 120
      if (!$Timeout) {
        $timelimit += $time
      }
      else {
        if ($timeLeft -lt $time) {
          $time = $timeLeft
        }
      }

      $status = "Waiting for $($time) seconds for nodes to register itself to ACM..."
      for ($i = 0; $i -lt $time; $i++) {
        if ($i -eq 0) {
          ShowProgress $startTime $timelimit $activity -Status $status -id 1
        }
        else {
          ShowProgress $startTime $timelimit $activity -Status $status -id 1 -nolog
        }
        Start-Sleep 1
      }
    }
  }

  if (!$RetainJobs) {
    ShowProgress $startTime $timelimit $activity -Status "Cleaning jobs..." -id 1
    $ids = $jobs.foreach('Id')
    Remove-AcmJob $ids
  }
  HideProgress 1

  ShowProgress $startTime $timelimit $activity -Status "Ending..." -id 1
  $result = CollectResult $names $jobs
  OutputResult $result
  if ($Return) {
    return $result
  }
}

function Add-AcmCluster {
<#
.SYNOPSIS
Add an Azure cluster of VMs/VM scale sets to ACM.

.PARAMETER ResourceGroup
The name of an Azure resource group containing the VMs/VM scale sets to test.

.PARAMETER AcmResourceGroup
The name of an Azure resource group containing the ACM service.

.PARAMETER SubscriptionId
The ID of an Azure subscription containing both the ResourceGroup and AcmResourceGroup.

.PARAMETER Timeout
The timeout value for adding cluster to Acm. By default, an estimated value will be set based on the number of VMs/VM scale sets in a cluster. A value shorter than necesssary will fail the setup procedure. You could specify a larger value to ensure the success of setup.

.PARAMETER RetainJobs
Do not remove PowerShell jobs after. This is for checking the job state for debug purpose.

.PARAMETER Return
Return the result. By default, the function returns nothing.

.PARAMETER LinuxExtensionUrl
URL for the Linux VM Extension. The file name in the URL must match pattern "^.+\-\d+\.\d+\.\d+\.\d+\.zip$". This option is internal for development.

.PARAMETER WindowsExtensionUrl
URL for the Windows VM Extension. The file name in the URL must match pattern "^.+\-\d+\.\d+\.\d+\.\d+\.zip$". This option is internal for development.

.NOTES
The command will log a lot to screen. So it's better to redirect them to files. Do it like
Add-AcmCluster ... 2>err_log 6>info_log
It writes errors to file err_log and other information to file info_log.

.EXAMPLE
Add-AcmCluster -SubscriptionId a486e243-747b-42de-8c4c-379f8295a746 -ResourceGroup 'my-cluster-1' -AcmResourceGroup 'my-acm-cluster' 2>err_log 6>info_log
Add a cluster of VMs/VM scale sets to ACM.
#>
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

    [string] $LinuxExtensionUrl,

    [string] $WindowsExtensionUrl
  )
  Initialize-AcmCluster @PSBoundParameters
}

function Remove-AcmCluster {
<#
.SYNOPSIS
Remove an Azure cluster of VMs/VM scale sets from ACM.

.PARAMETER ResourceGroup
The name of an Azure resource group containing the VMs/VM scale sets to test.

.PARAMETER AcmResourceGroup
The name of an Azure resource group containing the ACM service.

.PARAMETER SubscriptionId
The ID of an Azure subscription containing both the ResourceGroup and AcmResourceGroup.

.PARAMETER Timeout
The timeout value for adding cluster to Acm. By default, an estimated value will be set based on the number of VMs/VM scale sets in a cluster. A value shorter than necesssary will fail the setup procedure. You could specify a larger value to ensure the success of setup.

.PARAMETER RetainJobs
Do not remove PowerShell jobs after. This is for checking the job state for debug purpose.

.PARAMETER Return
Return the result. By default, the function returns nothing.

.EXAMPLE
Remove-AcmCluster -SubscriptionId a486e243-747b-42de-8c4c-379f8295a746 -ResourceGroup 'my-cluster-1' -AcmResourceGroup 'my-acm-cluster'
Remove a cluster of VMs/VM scale sets from ACM.
#>
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

  $pargs = @{
    startTime = $startTime
    timeout = $timeout
    activity = $activity
    status = $status
    id = $progId
  }
  $jobState = $null

  while ($true) {
    $pargs['op'] = "Job state: $($job.State)"
    if ($job.State -in "Finished", "Failed", "Canceled") {
      ShowProgress @pargs
      break
    }

    $elapsed = ($(Get-Date) - $startTime).TotalSeconds
    if ($elapsed -ge $Timeout) {
      ShowProgress @pargs
      Log $activity $status 'Timed out!'
      break
    }

    if ($jobState -ne $job.State) {
      ShowProgress @pargs
    }
    else {
      ShowProgress @pargs -nolog
    }
    $jobState = $job.State
    $job = Get-AcmDiagnosticJob -Id $job.Id -Connection $conn
    Start-Sleep 1
  }
  return $job
}

<#
.SYNOPSIS
Test Azure cluster of VMs/VM scale sets in ACM.

.PARAMETER ApiBasePoint
The URL of ACM web service. The value can be found by the result of Get-AcmAppInfo.

.PARAMETER IssuerUrl
The issuer URL of ACM web service, may be empty if the ACM web service is not protected by Azure AD. The value can be found by the result of Get-AcmAppInfo.

.PARAMETER ClientId
The client id of ACM web service, may be empty if the ACM web service is not protected by Azure AD. The value can be found by the result of Get-AcmAppInfo.

.PARAMETER ClientSecret
The client secret of ACM web service, may be empty if the ACM web service is not protected by Azure AD. The value can be found by the result of Get-AcmAppInfo.

.PARAMETER Timeout
The timeout value for performing test on cluster. By default, an estimated value will be set based on the number of nodes in a cluster. A value shorter than necesssary will cause no test result, since the test can't complete without enough time. You could specify a larger value to ensure the test to complete.

.PARAMETER Return
Return the result. By default, the function returns nothing.

.NOTES
The command will log a lot to screen. So it's better to redirect them to files. Do it like
Test-AcmCluster ... 2>err_log 6>info_log
It writes errors to file err_log and other information to file info_log.

.EXAMPLE
  $app = Get-AcmAppInfo -SubscriptionId 'my-id' -ResourceGroup 'my-group'; Test-AcmCluster @app 2>err_log 6>info_log
#>
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
  ShowProgress $startTime $timelimit $activity -Status $status -id 1

  $nodes = Get-AcmNode -Connection $conn -Count 100000
  $nodesInTest = $nodes.where({ $_.Health -eq 'OK' -and $_.State -eq 'Online' })
  $linuxNodeNames = $nodesInTest.where({ $_.NodeRegistrationInfo.DistroInfo -like '*Linux*' }).foreach('Name')
  $winNodeNames = $nodesInTest.where({ $_.NodeRegistrationInfo.DistroInfo -like '*Windows*' }).foreach('Name')
  $names = $linuxNodeNames + $winNodeNames

  Write-Host "There're $($nodes.Count) nodes in the cluster, among which there're $($linuxNodeNames.Count) Linux nodes and $($winNodeNames.Count) Windows nodes good for test."

  if ($names.Count -gt 0) {
    if (!$Timeout) {
      # Recompute timelimit based on node number.
      $timelimit = [Math]::Truncate($names.Count / $basesize) * $basetime
      if ($names.Count % $basesize -gt 0) {
        $timelimit += $basetime
      }
      $timelimit += 60 # Additional time for installation of prerequisites
    }

    # First, install necessary tools
    if ($linuxNodeNames.Count -gt 0) {
      $status = "Installing test prerequisites on Linux nodes..."
      ShowProgress $startTime $timelimit $activity -Status $status -id 1

      $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $linuxNodeNames -Category 'Prerequisite' -Name 'Intel MPI Installation'
      $job = Wait-AcmDiagnosticJob $job $conn $startTime $timelimit $activity $status -progId 1
      if ($job.State -ne 'Finished') {
        throw "Linux prerequisite installation failed."
      }
    }

    if ($winNodeNames.Count -gt 0) {
      $status = "Installing test prerequisites on Windows nodes..."
      ShowProgress $startTime $timelimit $activity -Status $status -id 1

      $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $winNodeNames -Category 'Prerequisite' -Name 'Microsoft MPI Installation'
      $job = Wait-AcmDiagnosticJob $job $conn $startTime $timelimit $activity $status -progId 1
      if ($job.State -ne 'Finished') {
        throw "Windows prerequisite installation timed out."
      }
    }

    # Then, do test
    $status = "Performing test on nodes..."
    ShowProgress $startTime $timelimit $activity -Status $status -id 1

    $job = Start-AcmDiagnosticJob -Connection $conn -Nodes $names -Category 'MPI' -Name 'Pingpong'
    $job = Wait-AcmDiagnosticJob $job $conn $startTime $timelimit $activity $status -progId 1
    if ($job.State -ne 'Finished') {
      throw "Test job failed."
    }

    # Finally, get aggreation result
    $status = "Fetching test aggregation result..."
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

  $status = "Generating result..."
  ShowProgress $startTime $timelimit $activity -Status $status -id 1

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
<#
.SYNOPSIS
Get ACM app/service info for use of Test-AcmCluster.

.PARAMETER ResourceGroup
The name of an Azure resource group containing the ACM service.

.PARAMETER SubscriptionId
The ID of an Azure subscription containing the ResourceGroup.
#>
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
<#
.SYNOPSIS
Add an Azure cluster of VMs/VM scale sets to ACM and perform MPI Pingpong test on it.

.PARAMETER ResourceGroup
The name of an Azure resource group containing the VMs/VM scale sets to test.

.PARAMETER AcmResourceGroup
The name of an Azure resource group containing the ACM service.

.PARAMETER SubscriptionId
The ID of an Azure subscription containing both the ResourceGroup and AcmResourceGroup.

.PARAMETER SetupTimeout
The timeout value for adding cluster to Acm. By default, an estimated value will be set based on the number of VMs/VM scale sets in a cluster. A value shorter than necesssary will fail the setup procedure. You could specify a larger value to ensure the success of setup.

.PARAMETER TestTimeout
The timeout value for performing test on cluster. By default, an estimated value will be set based on the number of nodes in a cluster. A value shorter than necesssary will cause no test result, since the test can't complete without enough time. You could specify a larger value to ensure the test to complete.

.PARAMETER NoSetup
Do not add cluster to ACM but only do test on it. This is for repeated test on a cluster that already has been added to ACM.

.PARAMETER LinuxExtensionUrl
URL for the Linux VM Extension. The file name in the URL must match pattern "^.+\-\d+\.\d+\.\d+\.\d+\.zip$". This option is internal for development.

.PARAMETER WindowsExtensionUrl
URL for the Windows VM Extension. The file name in the URL must match pattern "^.+\-\d+\.\d+\.\d+\.\d+\.zip$". This option is internal for development.

.NOTES
The command will log a lot to screen. So it's better to redirect them to files. Do it like
New-AcmTest ... 2>err_log 6>info_log
It writes errors to file err_log and other information to file info_log.

.EXAMPLE
New-AcmTest -SubscriptionId a486e243-747b-42de-8c4c-379f8295a746 -ResourceGroup 'my-cluster-1' -AcmResourceGroup 'my-acm-cluster' 2>err_log 6>info_log
Perform test on a cluster of VMs/VM scale sets that has not been added to ACM before. It also writes logs to files.

.EXAMPLE
New-AcmTest -SubscriptionId a486e243-747b-42de-8c4c-379f8295a746 -ResourceGroup 'my-cluster-1' -AcmResourceGroup 'my-acm-cluster' -NoSetup 2>err_log 6>info_log
Perform test on a cluster of VMs/VM scale sets that has been added to ACM already.
#>
  param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $AcmResourceGroup,

    [Parameter(Mandatory = $true)]
    [string] $SubscriptionId,

    [int] $SetupTimeout,

    [int] $TestTimeout,

    [switch] $NoSetup,

    [string] $LinuxExtensionUrl,

    [string] $WindowsExtensionUrl
  )

  if (!$NoSetup) {
    Log "Adding cluster to ACM service..."
    $args = @{
      SubscriptionId = $SubscriptionId
      ResourceGroup = $ResourceGroup
      AcmResourceGroup = $AcmResourceGroup
    }
    if ($SetupTimeout) {
      $args['Timeout'] = $SetupTimeout
    }
    Add-AcmCluster @args
  }

  Log "Getting ACM service app configuration..."
  $app = Get-AcmAppInfo -SubscriptionId $SubscriptionId -ResourceGroup $AcmResourceGroup
  if (!$app['IssuerUrl']) {
    Write-Warning "No authentication configuration is found for the ACM app in $($AcmResourceGroup)!"
  }

  Log "Testing cluster in ACM service..."
  if ($TestTimeout) {
    $app['Timeout'] = $TestTimeout
  }
  Test-AcmCluster @app
}
