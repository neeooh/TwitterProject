using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TwitterProject;

namespace TouristFilter
{
    class Program
    {
        static void Main(string[] args)
        {
            List<SingleTweet> HashtagTweets = new List<SingleTweet>();
            List<SingleTweet> GeoTweets = new List<SingleTweet>();

            //open filtered tweet location for geo and hashtag file


            //Group 1: Users with hometown set
            List<SingleTweet> Group1 = new List<SingleTweet>();

            //Group 2: Users with hometown outside of ireland
            List<SingleTweet> Group2 = new List<SingleTweet>();

            //Group 3: Users with null or unfound hometown
            List<SingleTweet> Group3 = new List<SingleTweet>();

            //Populate groups

            //Location null
            GeoTweets.ForEach((tweet) =>
            {
                if (tweet.user.location == null || tweet.user.location == "") {
                    Group3.Add(tweet);                    
                } else {
                    //Look up localization
                    Group1.Add(tweet);
                    //Found localisations

                    //Not found localisations
                }
            });
        }
    }
}
