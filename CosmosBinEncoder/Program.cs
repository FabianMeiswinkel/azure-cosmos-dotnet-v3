namespace CosmosBinEncoder
{
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Json;
    using Microsoft.Azure.Cosmos.Json.Interop;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal class Program
    {
        private static string oldDbName = "fabianm";
        private static bool stopTests = false;
        private static List<Tuple<String, int>> testCases = new List<Tuple<String, int>>()
        {
            Tuple.Create("BingDocs", 5000),
            Tuple.Create("DevTestDoc", 5000),
            Tuple.Create("InsertSimple10trm2048Kb", 1000),
            Tuple.Create("InsertSimple10trm1024Kb", 2000),
            Tuple.Create("InsertSimple10trm512Kb", 3000),
            Tuple.Create("InsertSimple10trm256Kb", 5000),
            Tuple.Create("InsertSimple10trm128Kb", 5000),
            Tuple.Create("InsertSimple10trm64Kb", 5000),
            Tuple.Create("InsertSimple10trm32Kb", 10000),
            Tuple.Create("InsertSimple10trm16Kb", 10000),
            Tuple.Create("InsertSimple10trm8Kb", 10000),
            Tuple.Create("InsertSimple10trm4Kb", 10000),
            Tuple.Create("InsertSimple10trm2Kb", 10000),
            Tuple.Create("InsertSimple10trm1Kb", 10000),
            Tuple.Create("InsertComplexIndexing", 5000),
            Tuple.Create("LastFm", 10000),
            Tuple.Create("MillionSongDoc", 10000),
            Tuple.Create("Nutrition", 10000),
            Tuple.Create("PerfComplex", 10000),
            Tuple.Create("PerfSimple", 10000),
        };

        static void Main(string[] args)
        {
            foreach (Tuple<string, int> testCase in testCases)
            {
                try
                {
                    Console.WriteLine("================================================================");
                    Console.WriteLine($" STARTING {testCase.Item1} with {testCase.Item2} iterations");
                    Console.WriteLine("================================================================");
                    MainAsync(args, testCase).Wait();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("EXCEPTION: {0}", ex);
                }
                finally
                {
                    Console.WriteLine();
                    Console.WriteLine("================================================================");
                    Console.WriteLine($" FINISHED {testCase.Item1} with {testCase.Item2} iterations");
                    Console.WriteLine("================================================================");
                    Console.WriteLine();
                    Console.WriteLine();
                }
            }

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
        }

        static async Task MainAsync(string[] args, Tuple<string, int> testCase)
        {
            if (args.Length != 2 || String.IsNullOrWhiteSpace(args[0]) || String.IsNullOrWhiteSpace(args[1]))
            {
                throw new ArgumentException("Command line arguments need to be the write-enabled connection strings for Text and Binary.", nameof(args));
            }

            String textConnectionString = args[0];
            String binaryConnectionString = args[1];
            CosmosClientOptions clientOptions = new CosmosClientOptions
            {
                EnableContentResponseOnWrite = true
            };

            using CosmosClient textClient = new CosmosClient(textConnectionString, clientOptions);
            using CosmosClient binaryClient = new CosmosClient(binaryConnectionString, clientOptions);
            
            string testCaseName = testCase.Item1;
            string inputFileName = testCaseName + ".json";
            string dbName = "fabianm_" + testCaseName;
            int maxIterationCount = testCase.Item2;

            await CreateTestContainer(textClient, "TextToTextWithResponse", dbName);
            await CreateTestContainer(textClient, "TextToTextNoResponse", dbName);
            await CreateTestContainer(binaryClient, "TextToBinaryWithResponse", dbName);
            await CreateTestContainer(binaryClient, "TextToBinaryNoResponse", dbName);
            await CreateTestContainer(binaryClient, "BinaryToBinaryWithResponse", dbName);
            await CreateTestContainer(binaryClient, "BinaryToBinaryNoResponse", dbName);

            List<Task> tasks = new List<Task>();

            TestCase textToText = new TestCase(
                "Text-To-Text",
                textClient.GetDatabase(dbName).GetContainer("TextToTextWithResponse"),
                textClient.GetDatabase(dbName).GetContainer("TextToTextNoResponse"),
                false,
                maxIterationCount,
                inputFileName);
            tasks.Add(textToText.ExecuteAsync());

            TestCase textToBinary = new TestCase(
                "Text-To-Binary",
                binaryClient.GetDatabase(dbName).GetContainer("TextToBinaryWithResponse"),
                binaryClient.GetDatabase(dbName).GetContainer("TextToBinaryNoResponse"),
                false,
                maxIterationCount,
                inputFileName);
            tasks.Add(textToBinary.ExecuteAsync());

            TestCase binaryToBinary = new TestCase(
                "Binary-To-Binary",
                binaryClient.GetDatabase(dbName).GetContainer("BinaryToBinaryWithResponse"),
                binaryClient.GetDatabase(dbName).GetContainer("BinaryToBinaryNoResponse"),
                true,
                maxIterationCount,
                inputFileName);
            tasks.Add(binaryToBinary.ExecuteAsync());

            await Task.WhenAll(tasks);

            Console.WriteLine("Finished");
        }

        static async Task CreateTestContainer(CosmosClient client, String name, string dbName)
        {
            if (oldDbName != null)
            {
                ResponseMessage deleteResponse = await client.GetDatabase(oldDbName).DeleteStreamAsync();
                if (deleteResponse.StatusCode != System.Net.HttpStatusCode.NotFound)
                {
                    deleteResponse.EnsureSuccessStatusCode();
                    await Task.Delay(5000);
                }

                oldDbName = null;
            }

            await client.CreateDatabaseIfNotExistsAsync(dbName);

            ContainerProperties containerProperties = new ContainerProperties
            {
                Id = name,
                PartitionKeyPath = "/id",
                //DefaultTimeToLive = -1,
                IndexingPolicy = new IndexingPolicy
                {
                    IndexingMode = IndexingMode.Consistent,
                    Automatic = true,
                    // IncludedPaths =
                    // {
                    //     new IncludedPath { Path = "/category/*" }
                    // },
                    ExcludedPaths =
                                {
                                    new ExcludedPath { Path = "/*" }
                                }
                }
            };

            await client
                .GetDatabase(dbName)
                .CreateContainerIfNotExistsAsync(
                    containerProperties,
                    ThroughputProperties.CreateManualThroughput(10000));
        }

        private class TestCase
        {
            private readonly string jsonTemplate;
            private readonly String name;
            private readonly Container withResponseContainer;
            private readonly Container noResponseContainer;
            private readonly bool emitBinary;
            private readonly String[] withResponseExistingIds = new String[10];
            private readonly String[] noResponseExistingIds = new String[10];
            private long withResponseExistingIdsCount = 0;
            private long noResponseExistingIdsCount = 0;
            private readonly int maxIterationCount;
            private static readonly Random rnd = new Random();

            public object SupportedSerializationFormats { get; private set; }

            public TestCase(String name, Container withResponseContainer, Container noResponseContainer, bool emitBinary, int maxIterationCount, string inputFileName)
            {
                this.name = name;
                this.withResponseContainer = withResponseContainer;
                this.noResponseContainer = noResponseContainer;
                this.emitBinary = emitBinary;
                this.maxIterationCount = maxIterationCount;
                this.jsonTemplate = File.ReadAllText(inputFileName);
            }

            public async Task ExecuteAsync()
            {
                List<Task> workloads = new List<Task>
                {
                    this.StartCreateWorkload(shouldExpectResponse: true),
                    this.StartCreateWorkload(shouldExpectResponse: false),
                    this.StartUpsertWorkload(existing: false, shouldExpectResponse: true),
                    this.StartUpsertWorkload(existing: false, shouldExpectResponse: false),
                    this.StartUpsertWorkload(existing: true, shouldExpectResponse: true),
                    this.StartUpsertWorkload(existing: true, shouldExpectResponse: false),
                    this.StartReadWorkload()
                };

                await Task.WhenAll(workloads);
            }

            private async Task StartCreateWorkload(bool shouldExpectResponse)
            {
                Console.WriteLine($"{this.name}: Create workload (expect response: {shouldExpectResponse}) started.");
                int i = 0;
                ItemRequestOptions requestOptions = new ItemRequestOptions()
                {
                    EnableContentResponseOnWrite = shouldExpectResponse,
                };

                Container container = shouldExpectResponse ? this.withResponseContainer : this.noResponseContainer;

                while (!stopTests && i < this.maxIterationCount)
                {
                    try
                    {
                        JObject item = this.CreatePayload();

                        using (Stream inputStream = this.emitBinary ? serializeToBinary(item) : serializeToText(item))
                        {
                            string id = item["id"].Value<string>();
                            ResponseMessage response = await container.CreateItemStreamAsync(
                                inputStream,
                                new PartitionKey(id),
                                requestOptions);
                            response.EnsureSuccessStatusCode();
                            if (shouldExpectResponse)
                            {
                                byte[] responseBlob;
                                using (MemoryStream responseStream = new MemoryStream())
                                {
                                    response.Content.CopyTo(responseStream);
                                    responseBlob = responseStream.ToArray();
                                }
                                Debug.Assert(responseBlob.Length > 1);
                                bool responseIsBinary = responseBlob[0] == (byte)JsonSerializationFormat.Binary;
                                Debug.Assert(responseIsBinary == this.emitBinary);
                            }
                            else
                            {
                                Debug.Assert(response.Content == null);
                            }
                            long existingIdsCountSnapshot = shouldExpectResponse 
                                ? Interlocked.Increment(ref this.withResponseExistingIdsCount)
                                : Interlocked.Increment(ref this.noResponseExistingIdsCount);
                            if (existingIdsCountSnapshot <= 10)
                            {
                                if (shouldExpectResponse)
                                {
                                    this.withResponseExistingIds[existingIdsCountSnapshot - 1] = id;
                                } 
                                else
                                {
                                    this.noResponseExistingIds[existingIdsCountSnapshot - 1] = id;
                                }
                            }
                        }

                        i++;
                        if (i % 100 == 0)
                        {
                            Console.WriteLine($"{this.name} - Create workload (expect response: {shouldExpectResponse}) Iterations: {i}");
                        }
                    }
                    catch (CosmosException cosmosError)
                    {
                        Console.WriteLine($"{this.name} - Create workload (expect response: {shouldExpectResponse}) Exception: {cosmosError}");
                        await Task.Delay(100);
                    }
                }

                Console.WriteLine($"{this.name}: Create workload (expect response: {shouldExpectResponse}) finished.");
            }

            private async Task StartUpsertWorkload(bool existing, bool shouldExpectResponse)
            {
                Console.WriteLine($"{this.name}: Upsert workload (existing: {existing}, expect resonse: {shouldExpectResponse}) started.");

                int i = 0;
                ItemRequestOptions requestOptions = new ItemRequestOptions()
                {
                    EnableContentResponseOnWrite = shouldExpectResponse,
                };

                Container container = shouldExpectResponse ? this.withResponseContainer : this.noResponseContainer;

                while (!stopTests && i < this.maxIterationCount)
                {
                    try
                    {
                        JObject item = CreatePayload();
                        if (existing)
                        {
                            if (shouldExpectResponse)
                            {
                                while (this.withResponseExistingIdsCount < 10)
                                {
                                    await Task.Delay(100);
                                }

                                item["id"] = this.withResponseExistingIds[rnd.Next(this.withResponseExistingIds.Length - 1)];
                            } else
                            {
                                while (this.noResponseExistingIdsCount < 10)
                                {
                                    await Task.Delay(100);
                                }

                                item["id"] = this.noResponseExistingIds[rnd.Next(this.noResponseExistingIds.Length - 1)];
                            }
                        }

                        using (Stream inputStream = this.emitBinary ? serializeToBinary(item) : serializeToText(item))
                        {
                            string id = item["id"].Value<string>();
                            ResponseMessage response = await container.UpsertItemStreamAsync(
                                inputStream,
                                new PartitionKey(id),
                                requestOptions);
                            response.EnsureSuccessStatusCode();
                            bool createdNewItem = response.StatusCode == System.Net.HttpStatusCode.Created;
                            Debug.Assert(createdNewItem == !existing);
                            if (shouldExpectResponse)
                            {
                                byte[] responseBlob;
                                using (MemoryStream responseStream = new MemoryStream())
                                {
                                    response.Content.CopyTo(responseStream);
                                    responseBlob = responseStream.ToArray();
                                }
                                Debug.Assert(responseBlob.Length > 1);
                                Boolean responseIsBinary = responseBlob[0] == (byte)JsonSerializationFormat.Binary;
                                Debug.Assert(responseIsBinary == this.emitBinary);
                            }
                            else
                            {
                                Debug.Assert(response.Content == null);
                            }
                            if (!existing)
                            {
                                long existingIdsCountSnapshot = shouldExpectResponse
                                ? Interlocked.Increment(ref this.withResponseExistingIdsCount)
                                : Interlocked.Increment(ref this.noResponseExistingIdsCount);
                                if (existingIdsCountSnapshot <= 10)
                                {
                                    if (shouldExpectResponse)
                                    {
                                        this.withResponseExistingIds[existingIdsCountSnapshot - 1] = id;
                                    }
                                    else
                                    {
                                        this.noResponseExistingIds[existingIdsCountSnapshot - 1] = id;
                                    }
                                }
                            }
                        }

                        i++;
                        if (i % 100 == 0)
                        {
                            Console.WriteLine($"{this.name} - Upsert workload (existing: {existing}, expect resonse: {shouldExpectResponse}) Iterations: {i}");
                        }
                    }
                    catch (CosmosException cosmosError)
                    {
                        Console.WriteLine($"{this.name} - Upsert workload (existing: {existing}, expect resonse: {shouldExpectResponse}) Exception: {cosmosError}");
                        await Task.Delay(100);
                    }
                }

                Console.WriteLine($"{this.name}: Upsert workload (existing: {existing}) finished.");
            }

            private async Task StartReadWorkload()
            {
                Console.WriteLine($"{this.name}: Read workload started.");

                int i = 0;
                while (!stopTests && i < this.maxIterationCount)
                {
                    try
                    {
                        String id;
                        while (this.withResponseExistingIdsCount < 10)
                        {
                            await Task.Delay(100);
                        }

                        id = this.withResponseExistingIds[rnd.Next(this.withResponseExistingIds.Length - 1)];

                        ItemRequestOptions requestOptions = new ItemRequestOptions
                        {
                            AddRequestHeaders = (headers) =>
                            {
                                headers["x-ms-cosmos-supported-serialization-formats"] = this.emitBinary ? "CosmosBinary" : "JsonText";
                                headers["x-ms-documentdb-content-serialization-format"] = this.emitBinary ? "CosmosBinary" : "JsonText";
                            }
                        };
                        ResponseMessage response = await this.withResponseContainer.ReadItemStreamAsync(
                            id,
                            new PartitionKey(id),
                            requestOptions);
                        response.EnsureSuccessStatusCode();
                        byte[] responseBlob;
                        using (MemoryStream responseStream = new MemoryStream())
                        {
                            response.Content.CopyTo(responseStream);
                            responseBlob = responseStream.ToArray();
                        }
                        Debug.Assert(responseBlob.Length > 1);
                        Boolean responseIsBinary = responseBlob[0] == (byte)JsonSerializationFormat.Binary;
                        Debug.Assert(responseIsBinary == this.emitBinary);

                        i++;
                        if (i % 100 == 0)
                        {
                            Console.WriteLine($"{this.name} - Read workload Iterations: {i}");
                        }
                    }
                    catch (CosmosException cosmosError)
                    {
                        Console.WriteLine($"{this.name} - Read workload Exception: {cosmosError}");
                        await Task.Delay(100);
                    }
                }

                Console.WriteLine($"{this.name}: Read workload finished.");
            }

            private static Stream serializeToBinary(JObject item)
            {
                using CosmosDBToNewtonsoftWriter writer = new CosmosDBToNewtonsoftWriter(JsonSerializationFormat.Binary);
                item.WriteTo(writer);
                return new MemoryStream(writer.GetResult().ToArray());
            }

            private static Stream serializeToText(JObject item)
            {
                MemoryStream stream = new MemoryStream();
                using (StreamWriter writer = new StreamWriter(
                    stream,
                    new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true),
                    1024,
                    leaveOpen: true))
                {
                    writer.WriteLine(item.ToString(Formatting.Indented));
                    writer.Flush();
                    stream.Flush();
                }

                stream.Position = 0;

                return stream;
            }

            private JObject CreatePayload()
            {
                JObject json = JObject.Parse(jsonTemplate);
                StringBuilder sb = new StringBuilder();
                json["id"] = Guid.NewGuid().ToString();
                json["mutableId"] = Guid.NewGuid().ToString();

                return json;
            }

        }
    }
}
