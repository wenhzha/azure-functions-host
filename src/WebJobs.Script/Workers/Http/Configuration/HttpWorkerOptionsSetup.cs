// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using Microsoft.Azure.WebJobs.Script.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Script.Workers.Http
{
    internal class HttpWorkerOptionsSetup : IConfigureOptions<HttpWorkerOptions>
    {
        private IConfiguration _configuration;
        private ILogger _logger;
        private ScriptJobHostOptions _scriptJobHostOptions;
        private string argumentsSectionName = $"{WorkerConstants.WorkerDescription}:arguments";
        private string workerArgumentsSectionName = $"{WorkerConstants.WorkerDescription}:workerArguments";

        public HttpWorkerOptionsSetup(IOptions<ScriptJobHostOptions> scriptJobHostOptions, IConfiguration configuration, ILoggerFactory loggerFactory)
        {
            _scriptJobHostOptions = scriptJobHostOptions.Value;
            _configuration = configuration;
            _logger = loggerFactory.CreateLogger<HttpWorkerOptionsSetup>();
        }

        public void Configure(HttpWorkerOptions options)
        {
            IConfigurationSection jobHostSection = _configuration.GetSection(ConfigurationSectionNames.JobHost);
            var httpWorkerSection = jobHostSection.GetSection(ConfigurationSectionNames.HttpWorker);
            var customHandlerSection = jobHostSection.GetSection(ConfigurationSectionNames.CustomHandler);

            if (httpWorkerSection.Exists() && customHandlerSection.Exists())
            {
                throw new HostConfigurationException($"Specifying both {ConfigurationSectionNames.HttpWorker} and {ConfigurationSectionNames.CustomHandler} in {ScriptConstants.HostMetadataFileName} file is not supported.");
            }

            if (customHandlerSection.Exists())
            {
                ConfigureOptionsHelper(options, customHandlerSection);
                return;
            }

            ConfigureOptionsHelper(options, httpWorkerSection);
        }

        private void ConfigureOptionsHelper(HttpWorkerOptions options, IConfigurationSection workerSection)
        {
            if (workerSection.Exists())
            {
                workerSection.Bind(options);
                HttpWorkerDescription httpWorkerDescription = options.Description;

                if (httpWorkerDescription == null)
                {
                    throw new HostConfigurationException($"Missing WorkerDescription");
                }

                var argumentsList = GetArgumentList(workerSection, argumentsSectionName);
                if (argumentsList != null)
                {
                    httpWorkerDescription.Arguments = argumentsList;
                }

                var workerArgumentList = GetArgumentList(workerSection, workerArgumentsSectionName);
                if (workerArgumentList != null)
                {
                    httpWorkerDescription.WorkerArguments = workerArgumentList;
                }

                httpWorkerDescription.ApplyDefaultsAndValidate(_scriptJobHostOptions.RootScriptPath, _logger);
                options.Arguments = new WorkerProcessArguments()
                {
                    ExecutablePath = options.Description.DefaultExecutablePath,
                    WorkerPath = options.Description.DefaultWorkerPath
                };

                options.Arguments.ExecutableArguments.AddRange(options.Description.Arguments);
                options.Port = GetUnusedTcpPort();
            }
        }

        private static List<string> GetArgumentList(IConfigurationSection httpWorkerSection, string argumentSectioName)
        {
            var argumentsSection = httpWorkerSection.GetSection(argumentSectioName);
            if (argumentsSection.Exists() && argumentsSection?.Value != null)
            {
                try
                {
                    return JsonConvert.DeserializeObject<List<string>>(argumentsSection.Value);
                }
                catch
                {
                }
            }
            return null;
        }

        internal static int GetUnusedTcpPort()
        {
            using (Socket tcpSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                tcpSocket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
                int port = ((IPEndPoint)tcpSocket.LocalEndPoint).Port;
                return port;
            }
        }
    }
}