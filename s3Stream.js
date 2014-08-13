var argv = require('optimist')
	.usage('Usage: $0 OPTIONAL: --bucketName [myBucket] --keyPrefix [folder/Server/] --filename [myFile] --accessKey [gibberish] --secretKey [gibberish]  --region [us-east-1]')
	.argv

process.env.AWS_API_KEY = process.env.AWS_API_KEY || argv.accessKey

process.env.AWS_SECRET_KEY = process.env.AWS_SECRET_KEY || argv.secretKey

if(!process.env.AWS_API_KEY
	|| !process.env.AWS_SECRET_KEY){
	console.log("The Access and Secret Keys for AWS must be set either through environment variables (AWS_API_KEY, AWS_SECRET_KEY) or through --accessKey, --secretKey options")
	process.exit(0)
}

process.env.AWS_BUCKET_NAME = process.env.AWS_BUCKET_NAME || argv.bucketName
if(!process.env.AWS_BUCKET_NAME){
	console.log("The AWS bucket must be set either through environment variables (AWS_BUCKET_NAME) or --bucketName option")
	process.exit(0)
}

process.env.AWS_REGION = process.AWS_REGION || argv.region
if(!process.env.AWS_REGION){
	//90% of the time, the user means this
	process.env.AWS_REGION = 'us-east-1'
}

var minimumChunkSize = argv.minChunkSize || 5
if(minimumChunkSize < 5){
	console.log("The minimum chunk size must be greater than or equal to 5mb")
	process.exit(0)
}

const partSize = 1024 * 1024 * minimumChunkSize // 5mb minimum chunk size save the last part.
const maxUploadRetries = argv.uploadRetries || 3


//From here down is the uploader portion of the code

var aws = require('aws-sdk')
aws.config.update({
	accessKeyId: process.env.AWS_API_KEY,
	secretAccessKey: process.env.AWS_SECRET_KEY,
	region: process.env.AWS_REGION
})
var s3 = new aws.S3()
var fs = require('fs')
var async = require('async')

var uploadParameters = {}
var taskEnded = false
var filename
var currentWriteStream

/*
	generateNewUploadParameters
		Generates required S3 parameters for the upload, including setting a new file key. File key is based on hostname/ip and current time.

	cb - function(err, params)
		err - error
		params - copy of the newly set uploadParameters,
*/
function generateNewUploadParameters(cb){
	//Generate a new key with the timestamp
	require('dns').lookup(require('os').hostname(), function (err, address, family) {
		if(err){
			cb(err)
			return
		}

		var now = new Date()
		filename = address + '_' + now.getDate() + '-' +
				'-' + now.getMonth() + '-' + now.getFullYear() + '_' + now.getHours() + ':' + now.getMinutes() + '_' + now.getTime() + '.log'

		var key
		if(argv.keyPrefix){
			key = argv.keyPrefix + '/' + filename
		} else {
			key = filename
		}



		uploadParameters = {
			Bucket: process.env.AWS_BUCKET_NAME,
			Key: key
			// ContentType: 'text/plain'
		}

		cb(null, uploadParameters, currentWriteStream)

	})

}

/*
	completeCurrentUpload
		Signals the completion of the upload to S3

	cb(err, data)
		err - error
		data - return value from s3
*/
function completeCurrentUpload(cb){
	s3.completeMultiPartUpload(uploadParameters, function(err, data){

		//uploadParameters need to be nullified to start a new upload
		uploadParameters = null

		cb(err, data)
	})
}

/*
	startNewMultiUpload
		Creates a new upload file. No data transferred

	cb(err, data)
		err- error
		data - return value from s3
*/
function startNewUpload(cb){
	generateNewUploadParameters(function(err, params){
		if(err){
			cb(err)
		} else {
			s3.createMultiPartUpload(uploadParameters, function(err, data){
				cb(err, data)
			})
		}
	})
}


/*
	uploadCurrentChunk
		Uploads a part of the log file

	function
*/
function uploadCurrentChunk(cb){
	
	var params = uploadParameters

	s3.uploadPart(uploadParameters, function(err, data){
		cb(err, data)
	})
}

//Handling queue
var queure = async.queue



function handleChunk(cb){
	async.waterfall([

		function(done){
			if(currentWriteStream){
				done(currentWriteStream)
			} else {
				startNewUpload(function(err){
					currentWriteStream = fs.createWriteStream('./' + filename + '_tmp')
					done(err)
				})
			}
		},

		function(writeStream, done){
			writeStream.write(chunk, 'utf8', function(success){
				done(null, success)
			})
		},

		function(success, done){
			if(!success){
				done(null)
			} else {
				fs.fstat('./' + filename + '_tmp', function(err, stats){
					if(err){
						done(err)
						return
					}

					if(stats.size >= minimumChunkSize){
						uploadCurrentChunk(function(err){
							done(err)
						})
					} else {
						done(null)
					}
				})
			}
		}

	], function(err){
		if(!argv.silent){
			process.stdout.write(chunk)
		}
		cb(err)
	})
}

var chunkQueue = async.queue(handleChunk, 1)
chunkQueue.drain = function(){

	//If the queue is empty, is our task ended? If so, end the upload and end the process
	if(taskEnded){
		completeCurrentUpload(function(err){
			process.exit(0)
		})
	}
}


//use stdIn as a stream
process.stdin.setEncoding('utf8')
process.stdin.on('readable', function(){
	var chunk = process.stdin.read()

	if(chunk !== null){
		chunkQueue.push(chunk)
	}

})

//On the stdIn's end, we need to set taskEnded to true. This notifies the writer that, on the last read, signal the file to be over to close out
process.stdin.on('end', function(){
	console.log("on end")
	taskEnded = true
})
process.stdin.on('close', function(){
	console.log("on close")
	taskEnded = true
})