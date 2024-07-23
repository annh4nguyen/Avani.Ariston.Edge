
namespace Avani.Getset.Service
{
    partial class ProjectInstaller
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.ServiceProcessInstaller = new System.ServiceProcess.ServiceProcessInstaller();
            this.AvaniAndonEdgeServiceInstaller = new System.ServiceProcess.ServiceInstaller();
            // 
            // ServiceProcessInstaller
            // 
            this.ServiceProcessInstaller.Account = System.ServiceProcess.ServiceAccount.LocalSystem;
            this.ServiceProcessInstaller.Password = null;
            this.ServiceProcessInstaller.Username = null;
            // 
            // AvaniAndonEdgeServiceInstaller
            // 
            this.AvaniAndonEdgeServiceInstaller.DelayedAutoStart = true;
            this.AvaniAndonEdgeServiceInstaller.Description = "The Edge Service Enables you to connect to gateway as client OR devices as server" +
    "";
            this.AvaniAndonEdgeServiceInstaller.DisplayName = "Avani Andon Edge Service";
            this.AvaniAndonEdgeServiceInstaller.ServiceName = "AvaniAndonEdgeService";
            this.AvaniAndonEdgeServiceInstaller.StartType = System.ServiceProcess.ServiceStartMode.Automatic;
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.ServiceProcessInstaller,
            this.AvaniAndonEdgeServiceInstaller});

        }

        #endregion

        private System.ServiceProcess.ServiceProcessInstaller ServiceProcessInstaller;
        private System.ServiceProcess.ServiceInstaller AvaniAndonEdgeServiceInstaller;
    }
}