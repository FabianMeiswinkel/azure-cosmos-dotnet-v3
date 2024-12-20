//-----------------------------------------------------------------------
// <copyright file="CrossPartitionQueryTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Json;
    using Microsoft.Azure.Cosmos.Tests.Json;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using static Microsoft.Azure.Cosmos.Json.JsonBinaryEncoding;

    /// <summary>
    /// Tests for CrossPartitionQueryTests.
    /// </summary>
    [TestClass]
    [TestCategory("Quarantine") /* Used to filter out quarantined tests in gated runs */]
    public class BinaryEncodingOverTheWireTests
    {
        private static readonly string[] NoDocuments = new string[] { };

        private static readonly CosmosClient GatewayClient = new CosmosClient(
            ConfigurationManager.AppSettings["GatewayEndpoint"],
            ConfigurationManager.AppSettings["MasterKey"],
            new CosmosClientOptions() { ConnectionMode = ConnectionMode.Gateway });
        private static readonly CosmosClient RntbdClient = new CosmosClient(
            ConfigurationManager.AppSettings["GatewayEndpoint"],
            ConfigurationManager.AppSettings["MasterKey"],
            new CosmosClientOptions() { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Documents.Client.Protocol.Tcp });
        private static readonly CosmosClient[] Clients = new CosmosClient[] { GatewayClient, RntbdClient };
        private static readonly CosmosClient Client = RntbdClient;
        private static readonly AsyncLazy<Database> Database = new AsyncLazy<Database>(async () => await Client.CreateDatabaseAsync(Guid.NewGuid().ToString()));

        private static async Task<Container> CreateContainerAsync()
        {
            return (await Database.Value).CreateContainerAsync(
                Guid.NewGuid().ToString() + "collection",
                "/id",
                10000).Result;
        }

        private static async Task<Tuple<Container, List<JToken>>> CreateCollectionAndIngestDocuments(IEnumerable<string> documents)
        {
            Container container = await BinaryEncodingOverTheWireTests.CreateContainerAsync();
            List<JToken> insertedDocuments = new List<JToken>();
            Random rand = new Random(1234);
            foreach (string serializedItem in documents.OrderBy(x => rand.Next()).Take(100))
            {
                JToken item = JToken.Parse(serializedItem);
                item["id"] = Guid.NewGuid().ToString();
                JToken createdItem = await container.CreateItemAsync<JToken>(item, new PartitionKey(item["id"].ToString()));
                insertedDocuments.Add(createdItem);
            }

            return new Tuple<Container, List<JToken>>(container, insertedDocuments);
        }

        internal delegate Task Query(CosmosClient cosmosClient, Container container, List<JToken> items);

        /// <summary>
        /// Task that wraps boiler plate code for query tests (collection create -> ingest documents -> query documents -> delete collections).
        /// Note that this function will take the cross product connectionModes and collectionTypes.
        /// </summary>
        /// <param name="connectionModes">The connection modes to use.</param>
        /// <param name="collectionTypes">The type of collections to create.</param>
        /// <param name="documents">The documents to ingest</param>
        /// <param name="query">
        /// The callback for the queries.
        /// All the standard arguments will be passed in.
        /// Please make sure that this function is idempotent, since a collection will be reused for each connection mode.
        /// </param>
        /// <param name="partitionKey">The partition key for the partition collection.</param>
        /// <param name="testArgs">The optional args that you want passed in to the query.</param>
        /// <returns>A task to await on.</returns>
        private static async Task CreateIngestQueryDelete(
            IEnumerable<string> documents,
            Query query)
        {
            Tuple<Container, List<JToken>> collectionAndDocuments = await BinaryEncodingOverTheWireTests.CreateCollectionAndIngestDocuments(documents);

            List<Task> queryTasks = new List<Task>();
            foreach (CosmosClient cosmosClient in BinaryEncodingOverTheWireTests.Clients)
            {
                queryTasks.Add(query(cosmosClient, collectionAndDocuments.Item1, collectionAndDocuments.Item2));
            }

            await Task.WhenAll(queryTasks);

            await collectionAndDocuments.Item1.DeleteContainerAsync();
        }

        private static async Task NoOp()
        {
            await Task.Delay(0);
        }

        [TestMethod]
        public void CheckThatAllTestsAreRunning()
        {
            // In general I don't want any of these tests being ignored or quarentined.
            // Please work with me if it needs to be.
            // I do not want these tests turned off for being "flaky", since they have been 
            // very stable and if they fail it's because something lower level is probably going wrong.

            Assert.AreEqual(0, typeof(BinaryEncodingOverTheWireTests)
                .GetMethods()
                .Where(method => method.GetCustomAttributes(typeof(TestMethodAttribute), true).Length != 0)
                .Where(method => method.GetCustomAttributes(typeof(TestCategoryAttribute), true).Length != 0)
                .Count(), $"One the {nameof(BinaryEncodingOverTheWireTests)} is not being run.");
        }

        [TestMethod]
        public async Task CombinedScriptsDataTest()
        {
            await this.TestCuratedDocs("CombinedScriptsData.json");
        }

        // For now we are skipping this test since the documents are too large to ingest and we get a rate size too large (HTTP 413).
#if TEST_COUNTRY
        [TestMethod]
        public async Task CountriesTest()
        {
            await this.TestCurratedDocs("countries");
        }
#endif

        [TestMethod]
        public async Task DevTestCollTest()
        {
            await this.TestCuratedDocs("devtestcoll.json");
        }

        [TestMethod]
        public async Task LastFMTest()
        {
            await this.TestCuratedDocs("lastfm");
        }

        [TestMethod]
        public async Task LogDataTest()
        {
            await this.TestCuratedDocs("LogData.json");
        }

        [TestMethod]
        public async Task MillionSong1KDocumentsTest()
        {
            await this.TestCuratedDocs("MillionSong1KDocuments.json");
        }

        [TestMethod]
        public async Task MsnCollectionTest()
        {
            await this.TestCuratedDocs("MsnCollection.json");
        }

        [TestMethod]
        public async Task NutritionDataTest()
        {
            await this.TestCuratedDocs("NutritionData");
        }

        [TestMethod]
        public async Task RunsCollectionTest()
        {
            await this.TestCuratedDocs("runsCollection");
        }

        [TestMethod]
        public async Task StatesCommitteesTest()
        {
            await this.TestCuratedDocs("states_committees.json");
        }

        [TestMethod]
        public async Task StatesLegislatorsTest()
        {
            await this.TestCuratedDocs("states_legislators");
        }

        [TestMethod]
        public async Task Store01Test()
        {
            await this.TestCuratedDocs("store01C.json");
        }

        [TestMethod]
        public async Task TicinoErrorBucketsTest()
        {
            await this.TestCuratedDocs("TicinoErrorBuckets");
        }

        [TestMethod]
        public async Task TwitterDataTest()
        {
            await this.TestCuratedDocs("twitter_data");
        }

        [TestMethod]
        public async Task Ups1Test()
        {
            await this.TestCuratedDocs("ups1");
        }

        [TestMethod]
        public async Task XpertEventsTest()
        {
            await this.TestCuratedDocs("XpertEvents");
        }

        private async Task TestCuratedDocs(string path)
        {
            IEnumerable<object> documents = BinaryEncodingOverTheWireTests.GetDocumentsFromCurratedDoc(path);
            await BinaryEncodingOverTheWireTests.CreateIngestQueryDelete(
                documents.Select(x => x.ToString()),
                this.TestCuratedDocs);
        }

        private async Task TestCuratedDocs(CosmosClient cosmosClient, Container container, List<JToken> items)
        {
            HashSet<JToken> inputItems = new HashSet<JToken>(items, JsonTokenEqualityComparer.Value);

            async Task AssertQueryDrainsCorrectlyAsync(FeedIterator<JToken> feedIterator)
            {
                while (feedIterator.HasMoreResults)
                {
                    FeedResponse<JToken> feedResponse = await feedIterator.ReadNextAsync();
                    foreach (JToken item in feedResponse)
                    {
                        Assert.IsTrue(inputItems.Contains(item), "Documents differ from input documents");
                    }
                }
            }

            FeedIterator<JToken> textFeedIterator = container.GetItemQueryIterator<JToken>(
                queryDefinition: new QueryDefinition("SELECT * FROM c ORDER BY c._ts"),
                requestOptions: new QueryRequestOptions()
                {
                    CosmosSerializationFormatOptions = new CosmosSerializationFormatOptions(
                        "JsonText",
                        (content) => JsonNavigator.Create(content),
                        () => Cosmos.Json.JsonWriter.Create(JsonSerializationFormat.Text)),
                });

            await AssertQueryDrainsCorrectlyAsync(textFeedIterator);

            FeedIterator<JToken> binaryFeedIterator = container.GetItemQueryIterator<JToken>(
                queryDefinition: new QueryDefinition("SELECT * FROM c ORDER BY c._ts"),
                requestOptions: new QueryRequestOptions()
                {
                    CosmosSerializationFormatOptions = new CosmosSerializationFormatOptions(
                        "CosmosBinary",
                        (content) => JsonNavigator.Create(content),
                        () => Cosmos.Json.JsonWriter.Create(JsonSerializationFormat.Text)),
                });

            await AssertQueryDrainsCorrectlyAsync(binaryFeedIterator);
        }

        private static IEnumerable<object> GetDocumentsFromCurratedDoc(string path)
        {
            path = string.Format("TestJsons/{0}", path);
            string json = TextFileConcatenation.ReadMultipartFile(path);
            List<object> documents;
            try
            {
                documents = JsonConvert.DeserializeObject<List<object>>(json);
            }
            catch (JsonSerializationException)
            {
                documents = new List<object>
                {
                    JsonConvert.DeserializeObject<object>(json)
                };
            }

            return documents;
        }

        [TestMethod]
        public void ExportJavaReferences_SByte()
        {
            sbyte[] values = new sbyte[] { SByte.MinValue, -99, 0, 1, 100, SByte.MaxValue};
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\SByte.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_Int64()
        {
            long[] values = new long[] { Int64.MinValue, (long)Int32.MinValue - 1, Int16.MinValue - 1, SByte.MinValue - 1, -99, 0, 1, 100, SByte.MaxValue + 1, Int16.MaxValue + 1, (long)Int32.MaxValue + 1, Int64.MaxValue };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\Int64.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_Int32()
        {
            int[] values = new int[] { Int32.MinValue, Int16.MinValue - 1, SByte.MinValue - 1 , - 99, 0, 1, 100, SByte.MaxValue + 1, Int16.MaxValue + 1, Int32.MaxValue };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\Int32.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_UInt32()
        {
            uint[] values = new uint[] { UInt32.MinValue, 1, 100, Byte.MaxValue + 1, UInt16.MaxValue + 1, UInt32.MaxValue };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\UInt32.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_Int16()
        {
            short[] values = new short[] { Int16.MinValue, SByte.MinValue - 1, -99, 0, 1, 100, SByte.MaxValue + 1, Int16.MaxValue };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\Int16.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_Float()
        {
            float[] values = new float[] { Single.MinValue, -(1/3f), -0.5f, 0, 0.5f, 1/3f, Single.MaxValue };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\Float.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_Double()
        {
            double[] values = new double[] { Double.MinValue, Single.MinValue - 0.000001, -(1 / 3), -0.5f, 0, 0.5f, 1 / 3, Single.MaxValue + -0.000001, Double.MaxValue };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\Double.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_String_Simple()
        {
            string[][] values = new[]
            {
                new string[]
                {
                    "Hello World",
                    "This is a test for a pretty long string exceeding one hundred characters in total length !!!!!!!!!!! Really!"
                }
            };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\String_Simple.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_JsonDoc_String_Simple()
        {
            JsonObject doc = new JsonObject
            {
                { "Property01", "HelloWorld" },
                { "Property02", "A" },
                { "Property03", new String('A', 998) },
                //{ "Property04", new String('B', 998) }
            };

            ExportJavaReferences(
                GenerateJavaReferencesAsDoc(doc),
                "C:\\Temp\\JsonDoc_String_Simple.json");
        }

        [TestMethod]
        public void ExportJavaReferences_JsonDoc_String_Trivial()
        {
            JsonObject doc = new JsonObject
            {
                { "Property01", "HelloWorld" },
            };

            ExportJavaReferences(
                GenerateJavaReferencesAsDoc(doc),
                "C:\\Temp\\JsonDoc_String_Trivial.json");
        }

        [TestMethod]
        public void ExportJavaReferences_JsonDoc_String_Multiple()
        {
            JsonObject doc = new JsonObject
            {
                { "id", "SomeId" },
                { "Property01", "HelloWorld" },
                { "Property02", "HelloWorld" },
                { "Property03", Guid.NewGuid().ToString() }
            };

            ExportJavaReferences(
                GenerateJavaReferencesAsDoc(doc),
                "C:\\Temp\\JsonDoc_String_Multiple.json");
        }

        [TestMethod]
        public void ExportJavaReferences_JsonDoc_Number_Simple()
        {
            JsonObject doc = new JsonObject
            {
                { "IntProperty", 12345 },
                { "FloatProperty", 123.45 }
            };

            ExportJavaReferences(
                GenerateJavaReferencesAsDoc(doc),
                "C:\\Temp\\JsonDoc_Number_Simple.json");
        }

        [TestMethod]
        public void ExportJavaReferences_String_SystemStrings()
        {
            string[][] values = new[]
            {
                new string[]
                {
                    "id",
                    "_ts",
                    "_rid",
                    "_self",
                    "_attachments",
                    "_ts"
                }
            };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\String_SystemStrings.json"
            );
        }

        [TestMethod]
        public void ExportJavaReferences_String_ReferenceStrings()
        {
            string[][] values = new string[][]
            {
                new string[]
                {
                    "ReferenceString01",
                    "ReferenceString01",
                    "ReferenceString01"
                },
                new string[]
                {
                    "ReferenceString01",
                    "ReferenceString02",
                    "ReferenceString03"
                }
            };
            ExportJavaReferences(
                GenerateJavaReferences(values),
                "C:\\Temp\\String_ReferenceStrings.json"
            );
        }

        private static void ExportJavaReferences(JsonObject doc, String fileName)
        {
            using (StreamWriter output = File.CreateText(fileName))
            {
                JsonSerializerOptions jsonOptions = new JsonSerializerOptions
                {
                    WriteIndented = true
                };
                output.WriteLine(doc.ToJsonString(jsonOptions));
            }
        }

        private static void WriteBinaryValue(IJsonWriter writer, sbyte value)
        {
            writer.WriteInt8Value(value);
        }

        private static void WriteBinaryValue(IJsonWriter writer, int value)
        {
            writer.WriteInt32Value(value);
        }

        private static void WriteBinaryValue(IJsonWriter writer, uint value)
        {
            writer.WriteUInt32Value(value);
        }

        private static void WriteBinaryValue(IJsonWriter writer, short value)
        {
            writer.WriteInt16Value(value);
        }

        private static void WriteBinaryValue(IJsonWriter writer, long value)
        {
            writer.WriteInt64Value(value);
        }

        private static void WriteBinaryValue(IJsonWriter writer, float value)
        {
            writer.WriteFloat32Value(value);
        }

        private static void WriteBinaryValue(IJsonWriter writer, double value)
        {
            writer.WriteFloat64Value(value);
        }

        private static void WriteBinaryValue(IJsonWriter writer, string[] values)
        {
            writer.WriteArrayStart();
            foreach (string value in values)
            {
                writer.WriteStringValue(value);
            }
            writer.WriteArrayEnd();
        }

        private static void WriteBinaryValue(IJsonWriter writer, Object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            throw new NotImplementedException($"Write value not implemented for type '{value.GetType().Name}'. ");
        }

        private static JsonObject GenerateJavaReferencesAsDoc(JsonObject doc)
        {
            string jsonValue = doc.ToJsonString(new JsonSerializerOptions { WriteIndented = false });

            // Binary
            byte[] binary = JsonTestUtils.ConvertTextToBinary(jsonValue);
            Console.WriteLine($"ValueLength: {JsonBinaryEncoding.GetValueLength(binary.AsMemory().Span.Slice(1))}");
            JsonObject envelope = new()
            {
                { "jsonValue", jsonValue },
                { "binaryValueBase64",  Convert.ToBase64String(binary, Base64FormattingOptions.None) }
            };

            return envelope;
        }

        private static JsonObject GenerateJavaReferences<T>(IEnumerable<T> values)
        {
            JsonArray valuesNode = new JsonArray();
            JsonObject doc = new()
            {
                { "type", typeof(T).Name },
                { "values", valuesNode }
            };

            foreach (T v in values)
            {
                IJsonWriter writer = Cosmos.Json.JsonWriter.Create(JsonSerializationFormat.Binary, JsonWriteOptions.None, 256);

                WriteBinaryValue(writer, (dynamic)v);

                string jsonValue;
                if (typeof(T) == typeof(string[]))
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append("[\"");
                    string[] stringValues = (string[])(object)v;
                    jsonValue = String.Join('|', stringValues);
                }
                else
                {
                    jsonValue = String.Format(CultureInfo.InvariantCulture, "{0}", v);
                }

                byte[] dummy = writer.GetResult().Span.ToArray();
                for (int i = 0; i < 10; i++)
                {
                    Console.WriteLine($"{i}: {DebugTypeMarker(dummy[i])}");
                }

                JsonObject valueNode = new()
                {
                    { "jsonValue", jsonValue },
                    { "binaryValueBase64",  Convert.ToBase64String(dummy, Base64FormattingOptions.None) }
                    //{ "binaryValueBase64",  Convert.ToBase64String(writer.GetResult().Span, Base64FormattingOptions.None) }
                };
                valuesNode.Add(valueNode);
            }

            return doc;
        }

        public static string DebugTypeMarker(byte b)
        {
            String type = $"{b} --> ";

            if (JsonBinaryEncoding.TypeMarker.IsArray(b))
            {
                type += "Array, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsBoolean(b))
            {
                type += "Boolean, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsCompressedString(b))
            {
                type += "CompressedString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsDateTimeString(b))
            {
                type += "DateTimeString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsEmptyArray(b))
            {
                type += "EmptyArray, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsEmptyObject(b))
            {
                type += "EmptyObject, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsEncodedLengthString(b))
            {
                type += "EncodedLengthString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsEncodedNumberLiteral(b))
            {
                type += "EncodedNumberLiteral, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsEncodedString(b))
            {
                type += "EncodedString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsFalse(b))
            {
                type += "False, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsFixedLengthNumber(b))
            {
                type += "FixedLengthNumber, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsGuid(b))
            {
                type += "Guid, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsGuidString(b))
            {
                type += "GuidString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsHexadecimalString(b))
            {
                type += "HexadecimalString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsNull(b))
            {
                type += "Null, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsNumber(b))
            {
                type += "Number, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsObject(b))
            {
                type += "Object, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsOneByteEncodedString(b))
            {
                type += "OneByteEncodedString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsOneByteEncodedUserString(b))
            {
                type += "OneByteEncodedUserString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsReferenceString(b))
            {
                type += "ReferenceString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsString(b))
            {
                type += "String, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsSystemString(b))
            {
                type += "SystemString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsTrue(b))
            {
                type += "True, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsTwoByteEncodedString(b))
            {
                type += "TwoByteEncodedString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsTwoByteEncodedUserString(b))
            {
                type += "TwoByteEncodedUserString, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsUserString(b))
            {
                type += "UserString, ";
            }

            if (!JsonBinaryEncoding.TypeMarker.IsValid(b))
            {
                type += "Invalid, ";
            }

            if (JsonBinaryEncoding.TypeMarker.IsVariableLengthString(b))
            {
                type += "VariableLengthString, ";
            }

            if (String.IsNullOrEmpty(type))
            {
                return "NO TYPE MARKER";
            }

            return type;
        }

        public sealed class AsyncLazy<T> : Lazy<Task<T>>
        {
            public AsyncLazy(Func<T> valueFactory) :
                base(() => Task.Factory.StartNew(valueFactory))
            { }

            public AsyncLazy(Func<Task<T>> taskFactory) :
                base(() => Task.Factory.StartNew(() => taskFactory()).Unwrap())
            { }
        }

        public sealed class JsonTokenEqualityComparer : IEqualityComparer<JToken>
        {
            public static JsonTokenEqualityComparer Value = new JsonTokenEqualityComparer();

            public bool Equals(double double1, double double2)
            {
                return double1 == double2;
            }

            public bool Equals(string string1, string string2)
            {
                return string1.Equals(string2);
            }

            public bool Equals(bool bool1, bool bool2)
            {
                return bool1 == bool2;
            }

            public bool Equals(JArray jArray1, JArray jArray2)
            {
                if (jArray1.Count != jArray2.Count)
                {
                    return false;
                }

                IEnumerable<Tuple<JToken, JToken>> pairwiseElements = jArray1
                    .Zip(jArray2, (first, second) => new Tuple<JToken, JToken>(first, second));
                bool deepEquals = true;
                foreach (Tuple<JToken, JToken> pairwiseElement in pairwiseElements)
                {
                    deepEquals &= this.Equals(pairwiseElement.Item1, pairwiseElement.Item2);
                }

                return deepEquals;
            }

            public bool Equals(JObject jObject1, JObject jObject2)
            {
                if (jObject1.Count != jObject2.Count)
                {
                    return false;
                }

                bool deepEquals = true;
                foreach (KeyValuePair<string, JToken> kvp in jObject1)
                {
                    string name = kvp.Key;
                    JToken value1 = kvp.Value;

                    if (jObject2.TryGetValue(name, out JToken value2))
                    {
                        deepEquals &= this.Equals(value1, value2);
                    }
                    else
                    {
                        return false;
                    }
                }

                return deepEquals;
            }

            public bool Equals(JToken jToken1, JToken jToken2)
            {
                if (object.ReferenceEquals(jToken1, jToken2))
                {
                    return true;
                }

                if (jToken1 == null || jToken2 == null)
                {
                    return false;
                }

                JsonType type1 = JTokenTypeToJsonType(jToken1.Type);
                JsonType type2 = JTokenTypeToJsonType(jToken2.Type);

                // If the types don't match
                if (type1 != type2)
                {
                    return false;
                }

                switch (type1)
                {

                    case JsonType.Object:
                        return this.Equals((JObject)jToken1, (JObject)jToken2);
                    case JsonType.Array:
                        return this.Equals((JArray)jToken1, (JArray)jToken2);
                    case JsonType.Number:
                        // NOTE: Some double values in the test document cannot be represented exactly as double. These values get some 
                        // additional decimals at the end. So instead of comparing for equality, we need to find the diff and check
                        // if it is within the acceptable limit. One example of such an value is 00324008. 
                        return Math.Abs((double)jToken1 - (double)jToken2) <= 1E-9;
                    case JsonType.String:
                        // TODO: Newtonsoft reader treats string representing datetime as type Date and doing a ToString returns 
                        // a string that is not in the original format. In case of our binary reader we treat datetime as string 
                        // and return the original string, so this comparison doesn't work for datetime. For now, we are skipping 
                        // date type comparison. Will enable it after fixing this discrepancy
                        if (jToken1.Type == JTokenType.Date || jToken2.Type == JTokenType.Date)
                        {
                            return true;
                        }

                        return this.Equals(jToken1.ToString(), jToken2.ToString());
                    case JsonType.Boolean:
                        return this.Equals((bool)jToken1, (bool)jToken2);
                    case JsonType.Null:
                        return true;
                    default:
                        throw new ArgumentException();
                }
            }

            public int GetHashCode(JToken obj)
            {
                return 0;
            }

            private enum JsonType
            {
                Number,
                String,
                Null,
                Array,
                Object,
                Boolean
            }

            private static JsonType JTokenTypeToJsonType(JTokenType type)
            {
                return type switch
                {
                    JTokenType.Object => JsonType.Object,
                    JTokenType.Array => JsonType.Array,
                    JTokenType.Integer or JTokenType.Float => JsonType.Number,
                    JTokenType.Guid or JTokenType.Uri or JTokenType.TimeSpan or JTokenType.Date or JTokenType.String => JsonType.String,
                    JTokenType.Boolean => JsonType.Boolean,
                    JTokenType.Null => JsonType.Null,
                    _ => throw new ArgumentException(),
                };
            }
        }
    }
}