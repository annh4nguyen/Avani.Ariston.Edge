using Avani.Andon.Edge.Logic;
using Avani.Helper;
using System;
using System.ServiceProcess;

namespace Avani.Andon.Edge.Service
{
    public partial class AndonEdgeService : ServiceBase
    {
        #region props
        private Log _Logger;
        private readonly string _LogCategory = "Service";
        private Logic.MainApp _AppLogic { get; set; }
        public AndonEdgeService()
        {
            _Logger = Avani.Andon.Edge.Logic.Helper.GetLog();
            _AppLogic = new Logic.MainApp();
            InitializeComponent();
        }
        #endregion
        protected override void OnStart(string[] args)
        {
            try
            {
                _Logger.Write(_LogCategory, "Andon Edge Service Start", LogType.Info);
                _AppLogic.Start();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Andon Edge Service Start Error: {ex}", LogType.Error);
                this.Stop();
            }
        }

        protected override void OnStop()
        {
            try
            {
                _Logger.Write(_LogCategory, "Andon Edge Service Stop", LogType.Info);
                _AppLogic.Stop();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Andon Edge Service Stop Error: {ex}", LogType.Error);
            }
        }
    }
}
