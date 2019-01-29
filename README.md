# PowerShell Module for HPC Pack ACM

The PowerShell module [HPC.ACM](https://www.powershellgallery.com/packages/HPC.ACM) is for performing basic MPI pingpong test for a cluster of VMs/VM scale sets on Azure. It adds the cluster to a deployed ACM service before the test.

## Prerequisites

To use the module, you have to get PowerShell and install the Azure PowerShell module `Az` first. Of course you must have deployed an ACM service.

### PowerShell

Either PowerShell Core 6.1 or Windows PowerShell 5.1 is OK. For older versions of PowerShell Core(>=6.0) and Windows PowerShell(>= 5.0), it may work but without guaranty.

PowerShell Core is available on Linux, Mac and Windows, while Windows PowerShell is only on Windows. See [the document here](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell?view=powershell-6) for how to install it on your platform.

### Az

[Az](https://www.powershellgallery.com/packages/Az/1.1.0) is the module for managing your resource on Azure. To install it, execute the following command under PowerShell:

```powershell
Install-Module -Name Az -Scope CurrentUser
```

## Installation

To install HPC.ACM, execute the following command under PowerShell:

```powershell
Install-Module -Name HPC.ACM -Scope CurrentUser
```

## Usage

To use the module to test your Azure cluster, execute the following command under PowerShell:

```powershell
New-AcmTest -SubscriptionId "YourSubscriptionId" -ResourceGroup "YourResourceGroupNameOfVmCluster" -AcmResourceGroup "YourResourceGroupNameOfAcmCluster" 2>error_log 6>info_log
```

Replace the arguments for yours. The command records errors in file "error_log" and information output in file "info_log", both under the current working directory.

Note: for your first time to access any resource on Azure in PowerShell, you will be prompted to authenticate to Azure by accessing a given URL with a given code. After the authentication, your session data is saved locally and you won't be prompted again even if you exit the PowerShell and enter one again on the same computer. So to run an automation script in PowerShell, you could first login to Azure(by `Login-AzAccount` from module `Az`) on the same computer to avoid being prompted in the future.

The above command will output result like below(with comments inside beginning with '#'):

```
###########################################################################
#
# Cluster setup result
# Each VM/VM scale set in the cluster is added to ACM by a PowerShell job.
# "Completed" means a job is completed or not. Note: even when a job is
# completed, the VM/VM scale set may still fail in adding to ACM. Usually,
# that's because required VM extension failed installing/starting on a Vm.
#
VM/VM Scale Set Completed JobId
--------------- --------- -----
centos7500           True     2
centos7501           True     3
centos7502           True     4
...

###################################
#
# Summary of cluster setup result
#
Total Completed Percent
----- --------- -------
   93        93 100.00%

############################################################################
#
# MPI Pingpong test result
# "Good for Test" means a node is able to and has participated in the test.
# "Good in MPI Pingpong" means a node passed the basic MPI Pingpong test.
#
Node       Good for Test Good in MPI Pingpong
----       ------------- --------------------
centos7500          True                 True
centos7501          True                 True
centos7502          True                 True
...

######################################
#
# Summary of MPI Pingpong test result
#
Total Good Percent
----- ---- -------
   93   93 100.00%

```

For more help on `New-AcmTest`, execute the following command under PowerShell:

```
help New-AcmTest -Detailed
```

### Run PowerShell Command outside of PowerShell

When you want to run PowerShell command without opening a PowerShell first, you could:

* For PowerShell Core under Linux/Mac

  ```bash
  pwsh -Command "New-AcmTest ... 2>err_log 6>info_log"
  ```

* For Windows PowerShell

  ```
  powershell -Command "..."
  ```

You could also save PowerShell commands in a `.ps1` file and execute it by:

* For PowerShell Core under Linux/Mac

  ```bash
  pwsh -File your-file-path
  ```

* For Windows PowerShell

  ```
  powershell -File your-file-path
  ```
