s3du
======================

s3du_parallel.rb reports disk usage by s3 bucket with multi-processes.
s3du_paralle.rb gives you the output of usage of all subdirectories within the specified bucket.

##Usage:

 $ s3du_parallel.rb bucketname [max_depth]

##Option:
 max_depth  Maxmum depth of the subdirectoriy to summarize; default is 2.

##Setup:

 # gem install parallel
 $ cat <<EOF > ~/.s3cfg
 > access_key = [your aws access key]
 > secret_key = [your aws secret access key]
 > EOF


License
----------
Copyright &copy; 2010-2012 So-net Entertainment Corporation  
Licensed under the [Apache License, Version 2.0][Apache]  

[Apache]: http://www.apache.org/licenses/LICENSE-2.0
