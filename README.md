# CommentsDataAnalysis
hadoop Program to analyse the comments data

Below is the Problm statement 
  1) First time a user commented on this site. 
  2) Last time the user commented on this site. 
  3) Total number of comments made by that user. 

# High level design for the issue 

1) Input data :  Comments.xml present in DataSet Repository
2) Expected Output  : 
          Userid 
          Date of First comment 
          Date of Last comment 
          Total number of comments. 
3) Deciding Input & Output key value pairs : 
          i) Input split {key, value} pair 
          ii) Output {key, value} pair 
              Key must be the userid itself. 
              Value would be date of first comment, date of last comment & the total number of comments. 
4) Mapper 
      XML parsing : Parse the extract details like User id, date of creation etc. 
      Write the parsed extracted details in the form of {key, value} pair. 
      The expected output value is first comment date, last comments date & the total count. So either we can have a STRING value which has been concatenated with list of output. But this output needed to be parsed by redicer again.   
      Otherwise its better to have a separate class that store all these values. So creating a class with multiple values as an object is better practice.  

5) Reducer 
      HDFS takes care of merge, shuffle, sorting etc. 
      Reduce will have {key, value} pair as the input. 
      Key is basically the userid & the value will be each individual object with first comment date, last comment date & the total comments. 
      Reducer output will look something like this. 

6) Reducer logic 

      So the reducer input is list of user ids & associated details. The logic must find the first date of comment. Look through set of dates in list of objects & find the earliest & the minimum dates among this.  
      Similarly last commented date can be found through set of dates in list of objects & find the maximum dates among them. 
      Increment the comment count. 
