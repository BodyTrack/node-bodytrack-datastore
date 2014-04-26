Bodytrack Datastore
===================

A Node.js interface for the BodyTrack Datastore (https://github.com/BodyTrack/datastore).

Installation
================================

1. This module requires an installation of the BodyTrack Datastore somewhere on your system.  If you don't have it already, do the following:

    1. Fetch the BodyTrack Datastore:

            git clone https://github.com/BodyTrack/datastore.git

    2. Follow the build and install instructions for the BodyTrack Datastore.  

2. Install this module in the usual way:

        npm install bodytrack-datastore
     
3. Do the following if you want to run this module's tests:

    1. Copy this project's `test/config.template.js` file to `test/config.js`.
    2. Edit `config.js` as appropriate for your installation of the BodyTrack Datastore.
    3. Run the tests:

            npm test

Usage
=====

This module provides a class named `BodyTrackDatastore`.  Create a new instance like this:

    var BodyTrackDatastore = require('./bodytrack-datastore');
    var datastore = new BodyTrackDatastore({
                                           binDir: "/PATH/TO/DATASTORE/BIN/DIRECTORY", 
                                           dataDir: "/PATH/TO/DATASTORE/DATA/DIRECTORY"
                                           });

The value for `binDir` should be the path to the directory containing the BodyTrack Datastore's binary executables (`export`, `gettile`, `import`, and `info`).  The value for `dataDir` should be the path to the BodyTrack Datastore's data directory (typically named `dev.kvs`).

For full documentation, generate the JSDocs:

    ./node_modules/.bin/jsdoc index.js
    
You'll find the generated docs in the `out` directory.