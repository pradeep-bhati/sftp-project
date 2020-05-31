# sftp-project
Sftp Inbound adpater in java configuration.
Code for adding moe than one filters using filter chain.
Setting up file pattern filters and persisting filter together while fetching from remote directory.
Setting up a local filter after file is read from remote directory and is present in file system.
(This filter will act b/w when file is picked from ur system and processed.)
Setting up 2 properties persisting metadata store, one for remote and one for local.
Setting up a mysql based metadatastore.
An outbound adapter which reads file picked up by inbound adapter and created remote directory name and 
File name , using header and payload expressions.
Setting up a new error channel to send exceptions.
