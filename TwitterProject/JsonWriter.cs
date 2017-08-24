using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterProject
{
    public class JsonWriter
    {
        private TextWriter _Writer;
        private string _FilePath;
        public bool finalCollection;

        public JsonWriter(string path)
        {
            
            _Writer = TextWriter.Synchronized(new StreamWriter(path));
            _FilePath = path;
            finalCollection = false;
            writeSingleLine("["); // "[" inidicates start of the list in JSON
        }

        public void serialiseCollection(ConcurrentBag<SingleTweet> inputObject, bool finalCollection = false)
        {
            string tweetSeparator = ",";

            for (int i = 0; i < inputObject.Count; i++)
            {
                if (finalCollection && i == inputObject.Count - 1)
                    tweetSeparator = "]";

                var outputBuf = JsonConvert.SerializeObject(inputObject.ElementAt<SingleTweet>(i), Formatting.Indented) + tweetSeparator;
                writeSingleLine(outputBuf);
            }
            
        }

        //public void serialiseTweet(SingleTweet inputObject)
        //{
        //    var outputBuf = JsonConvert.SerializeObject(inputObject, Formatting.Indented) + ",";
        //    writeSingleLine(outputBuf);
        //}

        //public void validateJson()
        //{
        //    removeLastCharFromFile();
        //}

        private void writeSingleLine(string outputString)
        {
            _Writer.WriteLine(outputString);
            _Writer.FlushAsync();
        }

        private void removeLastCharFromFile()
        {
            FileStream fs = new FileStream(_FilePath, FileMode.Open, FileAccess.ReadWrite);
            fs.SetLength(fs.Length - 10);
            fs.Close();
        }

    }
}
