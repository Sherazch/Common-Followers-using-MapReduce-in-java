# Common-Followers-using-MapReduce-in-java
MapReduce program which gets a sample of twitter following graph and produces an output which shows all their common followers for each pair of users. 

The input dataset can be downloaded from here: https://snap.stanford.edu/data/higgs- social_network.edgelist.gz and is in the following form: 
 
<user_id1> <user_id2) 
Where <user_id1> follows <user_id2>.
 
 
our output would be in the following form: 
 
<user_id_1> <user_id2> <A comma separated list of common followers> 
