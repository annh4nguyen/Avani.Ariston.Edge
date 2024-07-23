using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using Avani.Andon.Edge.Logic;

namespace Avani.Andon.Edge.Service
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new AndonEdgeService()
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
