Steps to Execute the Phase1
1.Execute Python script for getting the tweets from twitter.
2.Save the tweets into a json file locally.
3.Start hadoop and yarn and check whether all nodes are working with the help of jps command.
4.Put the json file into hdfs.
5.Now execute the word count command with hadoop and save the output into a file store in hdfs.
6.Execute the word count command with the spark and save the word count in an text file locally.  