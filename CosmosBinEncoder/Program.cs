namespace CosmosBinEncoder
{
    using System.Text;
    using System.Text.Json;
    using System.Text.Json.Serialization;
    using Microsoft.Azure.Cosmos.Core;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Json;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using CosmosJson = Microsoft.Azure.Cosmos.Json;

    internal class Program
    {
        static void Main(string[] args)
        {
            try
            {
                MainAsync(args).Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine("EXCEPTION: {0}", ex);
            }

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
        }

        static async Task MainAsync(string[] args)
        {
            if (args.Length == 0 || String.IsNullOrWhiteSpace(args[0]))
            {
                throw new ArgumentException("First command line argument needs to be the read-only connection string.", "args[0]");
            }

            FileInfo fileInfo = new FileInfo(args[0]);
            if (!fileInfo.Exists)
            {
                throw new ArgumentException($"Filename '{fileInfo.FullName}' does not exist.", "args[0]");
            }

            String jsonRawInput = String.Empty;
            using (StreamReader reader = File.OpenText(fileInfo.FullName))
            {
                jsonRawInput = reader.ReadToEnd();
            }

            JObject jObject = JsonConvert.DeserializeObject<JObject>(jsonRawInput);
            String jsonInput = JsonConvert.SerializeObject(jObject, Formatting.None);

            CosmosElement cosmosElement = CosmosElement.Parse(jsonInput);

            IJsonWriter writer = CosmosJson.JsonWriter.Create(JsonSerializationFormat.Text);
            cosmosElement.WriteTo(writer);
            WriteToFile(
                new FileInfo(Path.Combine(fileInfo.Directory.FullName, fileInfo.Name + ".cosmostext.json")),
                writer.GetResult());

            writer = CosmosJson.JsonWriter.Create(JsonSerializationFormat.Binary, enableEncodedStrings: true);
            cosmosElement.WriteTo(writer);
            WriteToFile(
                new FileInfo(Path.Combine(fileInfo.Directory.FullName, fileInfo.Name + ".binaryWithReferenceStrings.bin")),
                writer.GetResult());

            writer = CosmosJson.JsonWriter.Create(JsonSerializationFormat.Binary, enableEncodedStrings: false);
            cosmosElement.WriteTo(writer);
            WriteToFile(
                new FileInfo(Path.Combine(fileInfo.Directory.FullName, fileInfo.Name + ".binaryNoReferenceStrings.bin")),
                writer.GetResult());
        }

        static void WriteToFile(FileInfo fileInfo, ReadOnlyMemory<byte> payload)
        {
            if (fileInfo.Exists)
            {
                fileInfo.Delete();
            }

            using (FileStream file = fileInfo.Create())
            {
                file.Write(payload.Span);
                file.Flush();
            }
        }
    }
}
