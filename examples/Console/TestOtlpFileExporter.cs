// <copyright file="TestOtlpFileExporter.cs" company="OpenTelemetry Authors">
// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using System.Diagnostics;
using System.Net.Http;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace Examples.Console
{
#if DEBUG
    internal static class TestOtlpFileExporter
    {
        internal static object Run(string fileName)
        {
            return RunWithActivitySource(fileName);
        }

        private static object RunWithActivitySource(string fileName)
        {
            /*
             * launch the OTLP example by running:
             *
             *     dotnet run -p Examples.Console.csproj otlpfile
             *
             */

            using var openTelemetry = Sdk.CreateTracerProviderBuilder()
                    .AddHttpClientInstrumentation()
                    .AddOtlpTextWriterExporter(fileName)
                    .Build();

            var source = new ActivitySource("http-client-test");
            using (var parent = source.StartActivity("incoming request", ActivityKind.Server))
            {
                using var client = new HttpClient();
                client.GetStringAsync("http://bing.com").GetAwaiter().GetResult();
            }

            System.Console.WriteLine("Done. Hit any key to finish.");

            return null;
        }
    }
#endif
}
