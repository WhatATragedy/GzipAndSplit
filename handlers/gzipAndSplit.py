import tarfile
import os.path
import errno

#based on this https://www.oreilly.com/library/view/programming-python-second/0596000855/ch04s02.html

class GzipAndSplit():
    def __init__(self):
        self._CHUNK_SIZE = int(1.5 * 1024**3)
        kilobytes = 1024
        megabytes = kilobytes * 1000
        self._CHUNK_SIZE = int(40 * megabytes) 
    
    def gzipAndSplit(self, inputDirectory, outputDirectory):
        print("Welcome to Gzip And Split")
        try:
            os.mkdir(f"{outputDirectory}")
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
        zipFile = self.gzip(inputDirectory, "ribs.tar.gz")
        self.split(zipFile, outputDirectory)
        
    def gzip(self, inputDirectory, outputFileName):
        print("Zipppppping....")
        with tarfile.open(outputFileName, "w:gz") as tar:
            tar.add(inputDirectory, arcname=os.path.basename(inputDirectory))
        return outputFileName

    def split(self, inputFile, outputDirectory, chunksize=None):
        if not os.path.exists(outputDirectory):                  # caller handles errors
            os.mkdir(outputDirectory)                            # make dir, read/write parts
        else:
            for fname in os.listdir(outputDirectory):            # delete any existing files
                os.remove(os.path.join(outputDirectory, fname)) 

        chunksize = chunksize if chunksize is not None else self._CHUNK_SIZE
        partnum = 0
        with open(inputFile, 'rb') as infile:
            while True:
                chunk = infile.read(chunksize)
                if not chunk:
                    break
                partnum  = partnum+1
                filename = os.path.join(outputDirectory, ('part%04d' % partnum))
                fileobj  = open(filename, 'wb')
                fileobj.write(chunk)
                fileobj.close()
        return partnum

    def join(self, inputDirectory, outputFile, readsize=None):
        readsize = readsize if readsize is not None else self._CHUNK_SIZE
        output = open(outputFile, 'wb')
        parts  = os.listdir(inputDirectory)
        parts.sort(  )
        for filename in parts:
            filepath = os.path.join(inputDirectory, filename)
            fileobj  = open(filepath, 'rb')
            while 1:
                filebytes = fileobj.read(readsize)
                if not filebytes: break
                output.write(filebytes)
            fileobj.close(  )
        output.close(  )

