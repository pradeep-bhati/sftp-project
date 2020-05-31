# sftp-project
1) Sftp Inbound adpater in java configuration.
2) Code for adding moe than one filters using filter chain.
3) Setting up file pattern filters and persisting filter together while fetching from remote directory.
4) Setting up a local filter after file is read from remote directory and is present in file system.
  (This filter will act b/w when file is picked from ur system and processed.)
5) Setting up 2 properties persisting metadata store, one for remote and one for local.
6) Setting up a mysql based metadatastore.( This was having problem as table has to be created manually in MySQL)
7) An outbound adapter which reads file picked up by inbound adapter and created remote directory name and 
  File name , using header and payload expressions.
8) Setting up a new error channel to send exceptions.
