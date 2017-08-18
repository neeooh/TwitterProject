using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.BZip2;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.Threading;

namespace TwitterProject
{
    class Program
    {
        static void Main(string[] args)
        {
            //To-Do
            // Run the program in the loop, asking the user if he wants to process next folder
            Queue<string> rootDirQueue = new Queue<string>();
            bool stillDirInput = true;

            while (stillDirInput)
            {
                Console.WriteLine(@"Enter a path to the month you want to filter, eg. C:\TwitterProject\2017\01");
                string userInput = Console.ReadLine();
                
                if (userInput == "") //enter key was pressed with no input
                {
                    stillDirInput = true;
                    continue;
                }
                else
                {
                    rootDirQueue.Enqueue(userInput);
                    Console.WriteLine("Path added to the queue!");
                    Console.WriteLine("Would you like to process another directory? (y/n)");
                    userInput = Console.ReadLine();
                    if (userInput == "n")
                    {
                        stillDirInput = false; //terminate
                    }
                }
            };
            
            //Process the Queue
            foreach (string rootDir in rootDirQueue)
            {
                getGeoTaggedTweetsFromArchive(rootDir);
            }

            Console.ReadLine();
        }

        static void getGeoTaggedTweetsFromArchive(string path)
        {
            //Thread safe collection of Tweets which passed the filter criteria
            Stopwatch watch = Stopwatch.StartNew();
            ConcurrentBag<SingleTweet> geoTaggedTweetsBag = new ConcurrentBag<SingleTweet>();
            ConcurrentBag<SingleTweet> geoTaggedTweetsInPolygonBag = new ConcurrentBag<SingleTweet>();
            string failedPaths = "";
            int tweetsReadCounter = 0;
            int tweetsReadCounter2 = 0;


            DirectoryInfo JsonFileFolder = new DirectoryInfo(path);
            
            //Filter given directory for bz2 files and add them to a list of files
            var files = from file in JsonFileFolder.EnumerateFiles("*", SearchOption.AllDirectories)
                        where file.Extension == ".bz2"
                        select new
                        {
                            FileDir = file
                        };

            //In Parallel:
            //Decompress each .bz2 file, read lines and extract tweets with embedded geo-location
            Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = -1 }, (file) =>
            {
                Console.WriteLine("Processing {0}", file.FileDir.FullName);

                //Open file stream to a compressed bz2 file
                using (FileStream compressedStream = new FileStream(file.FileDir.FullName, FileMode.Open))
                {
                    //Create a byte buffer holding uncompressed stream bytes
                    //For safety purposes the length of uncompressed stream is 30 to account the size increase when file is decompressed
                    //There must be a better way to do it, BZip2.Decompress doesnt seem to accept resizable buffers?
                    //Memory is not a big issue for now anyway
                    byte[] uncompressedBuffer = new byte[compressedStream.Length * 20];

                    //Creates memory stream to hold the uncompressed data           
                    MemoryStream uncompressedStream = new MemoryStream(uncompressedBuffer);

                    //Decompresses into memory stream which stores bytes in uncompressedBuffer
                    try
                    {
                        BZip2.Decompress(compressedStream, uncompressedStream, true);
                    }
                    catch (Exception e)
                    {
                        failedPaths += file.FileDir.DirectoryName + "-" + file.FileDir.FullName + " \n";
                    }
                    //Long string containing the uncompressed data
                    string sUncompressed = Encoding.UTF8.GetString(uncompressedBuffer);

                    //Read Lines where each line represents a tweet
                    var inputLines = new BlockingCollection<string>();

                    var readLines = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            //Split the string into lines and add them to a list
                            var result = Regex.Split(sUncompressed, "\r\n|\r|\n");
                            foreach (var line in result)
                            {
                                Interlocked.Increment(ref tweetsReadCounter);
                                tweetsReadCounter2++;
                                inputLines.Add(line);
                            }
                            inputLines.CompleteAdding();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception occured while splitting a line from uncompressed text stream.");
                        }
                             
                    });

                    //Try to deserialize each JSON text line from the list
                    var processLines = Task.Factory.StartNew(() =>
                    {
                        Parallel.ForEach(inputLines.GetConsumingEnumerable(), line =>
                        { 
                            try
                            {
                                //Deserialize each line into a tweet object
                                var tweet = JsonConvert.DeserializeObject<SingleTweet>(line);

                                //BUSINESS LOGIC operations here 

                                // We are only interested in geo-tagged tweets
                                if (tweet.geo != null)
                                {
                                    // If the tweet is geo-tagged add it to the list
                                    geoTaggedTweetsBag.Add(tweet);

                                    if (PolygonFrame.pointInPolygon(tweet.geo.coordinates[0], tweet.geo.coordinates[1]))
                                    {
                                        geoTaggedTweetsInPolygonBag.Add(tweet);
                                    }
                                }
                                          
                            }
                            catch (Exception e)
                            {
                                // This exception occurs when you have a line that doesn't represent a tweet ({"created_at":) but a deleted tweet ({"delete":))
                                //Console.WriteLine("Exception occured while processing a line in {0}{1}", path, file.FileDir.Name);
                            }
                        });
                    });

                    //Wait till the file has been fully processed
                    Task.WaitAll(readLines, processLines);
                    Console.WriteLine("Finished reading " + file.FileDir.Name);
                }
            });
            
            watch.Stop();
            //Task.WaitAll(fileTask);

            //Save the geoTaggedTweetsBag object to .JSON file
            serializeToFile(path + "_geotagged.json", geoTaggedTweetsBag);
            serializeToFile(path + "_geotagged_in_polygon.json", geoTaggedTweetsInPolygonBag);
            //Save the statistics file
            saveStatisticsFile(path, watch, tweetsReadCounter, geoTaggedTweetsBag.Count, geoTaggedTweetsInPolygonBag.Count, failedPaths);
        }

        public static void serializeToFile(string path, ConcurrentBag<SingleTweet> objectToSerialize)
        {
            //Serialize the concurrentBag (all geo-tagged tweets) and save it as a .JSON file
            using (StreamWriter sw = new StreamWriter(path))
            {
                sw.Write(JsonConvert.SerializeObject(objectToSerialize, Formatting.Indented));
            }
        }

        private static void saveStatisticsFile(string rootPath, Stopwatch watch, int tweetsReadCounter, int geoTweetsCount, int geoTweetsInPolygonCount, string failedPaths)
        {
            try
            {
                //Save statistics.txt file
                using (StreamWriter sw = new StreamWriter(rootPath + "_statistics.txt"))
                {
                    if (failedPaths == "")
                        failedPaths = "0";

                    sw.WriteLine("Filtered tweets are form path: {0}", rootPath);
                    sw.WriteLine("Finished executing in: {0}ms | {1}mins | {2}h", 
                        watch.ElapsedMilliseconds, watch.Elapsed.Minutes, watch.Elapsed.Hours);
                    sw.WriteLine("totalProcessedTweets={0}", tweetsReadCounter);
                    sw.WriteLine("geoTaggedTweets={0}", geoTweetsCount);
                    sw.WriteLine("geoTaggedTweetsInPolygon={0}", geoTweetsInPolygonCount);
                    sw.WriteLine("failedArchives=" + failedPaths);
                    sw.WriteLine("<End of file>");
                }

                Console.WriteLine(@"Saved filtered fies to " + rootPath + ".");
            }
            catch (Exception e)
            {
                Console.WriteLine(@"Could not save to selected path. {0}. \nPlease try again.", e);
                rootPath = Console.ReadLine();
                saveStatisticsFile(rootPath, watch, tweetsReadCounter, geoTweetsCount, geoTweetsInPolygonCount, failedPaths);
            }
        }

    }
}
