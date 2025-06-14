package part4coordination

import cats.effect.kernel.Resource
import cats.effect.std.CountDownLatch
import cats.effect.{IO, IOApp}
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import scala.concurrent.duration.*
import utils.*
import java.io.{File, FileWriter}
import scala.io.{BufferedSource, Source}
import scala.util.Random

/**
 * A countdown latch is a concurrency primitive that has a count and two methods
 * called await and release. All threads (fibers) that call await() are
 * semantically blocked. We can decrease that initial count from some other
 * fibers via the release API call. When the internal count goes down to zero,
 * all fibers that are semantically blocked will proceed.
 *
 * Best use case: block a number of fibers until someone else gives the
 * go-ahead to proceed, like the firing gun at the start of a race.
 */
object CountdownLatches extends IOApp.Simple {

  type Nat = Int // my own type alias to represent natural number >= 0

  /**
   * Helper method that counts down from the given number of seconds and
   * releases a latch every second. count should be > 0.
   */
  def countdown(count: Nat, latch: CountDownLatch[IO]): IO[Unit] = for {
    _ <- IO(s"$count...").myDebug >> (IO sleep 1.second)
    _ <- latch.release // this is the pistol firing when count == 1
    _ <- if (count == 1) IO.unit else countdown(count - 1, latch)
  } yield ()

  /**
   * The trigger is the announcer for the race.
   */
  def announcer(latch: CountDownLatch[IO]): IO[Unit] =  for {
    _ <- IO("starting race shortly...").myDebug >> (IO sleep 2.seconds)
    _ <- countdown(5, latch)
    _ <- IO("GO GO GO").myDebug
  } yield ()

  def createRunner(id: Nat, latch: CountDownLatch[IO]): IO[Unit] = for {
    _ <- IO(s"[runner $id] waiting for signal...").myDebug
    _ <- latch.await  // blocks this fiber until the latch count reaches 0
    _ <- IO(s"[runner $id] Running!").myDebug
  } yield ()

  // Start a race with 10 runners and a latch count of 5
  def sprint(): IO[Unit] = for {
    latch         <- CountDownLatch[IO](5)
    announcerFib  <- announcer(latch).start
    _             <- (1 to 10).toList parTraverse (createRunner(_, latch))
    _             <- announcerFib.join
  } yield ()

  /**
   * Output of sprint():
   *
   * [io-compute-3] starting race shortly...
   * [io-compute-3] [runner 1] waiting for signal...
   * [io-compute-2] [runner 2] waiting for signal...
   * ...
   * [io-compute-3] [runner 10] waiting for signal...
   * [io-compute-3] 5...
   * [io-compute-3] 4...
   * [io-compute-3] 3...
   * [io-compute-3] 2...
   * [io-compute-3] 1...
   * [io-compute-3] GO GO GO
   * [io-compute-0] [runner 10] Running!
   * [io-compute-3] [runner 6] Running!
   * [io-compute-0] [runner 9] Running!
   * [io-compute-3] [runner 3] Running!
   * [io-compute-1] [runner 5] Running!
   * [io-compute-1] [runner 4] Running!
   * [io-compute-0] [runner 8] Running!
   * [io-compute-0] [runner 1] Running!
   * [io-compute-0] [runner 7] Running!
   * [io-compute-3] [runner 2] Running!
   */

  //---------------------------------------------------------------------------
  /**
   * Exercise. Simulate a multithreaded file downloader on a fictitious server.
   *
   * Fetch all the chunks of the file in parallel on separate fibers. Each
   * fiber will be written as a temporary file on disk. When all the chunks
   * have finished downloading, we will create another effect to stitch them
   * together into a single file. We will need a countdown latch to do it.
   */
  object FileServer {

    // In-memory file chunks. "Download" these and save them as separate files
    // on disk. Array indices run from 0 to n-1.
    val fileChunkList = Array(
      "I love Scala.",
      "Cats Effect seems quite fun.",
      "Never would I have thought I would do low-level concurrency with pure FP."
    )

    // API
    def getNumChunks:         IO[Nat]     = IO(fileChunkList.length)
    def getFileChunk(n: Nat): IO[String]  = IO(fileChunkList(n))
  }

  //===========================================================================
  // Methods related to DOWNLOADING file parts

  // Write contents to file on disk
  def writeToFile(path: String, contents: String): IO[Unit] = {
    val fileResource =
      Resource.make { IO(new FileWriter(new File(path)))  } // acquire resource
        { writer => IO(writer.close())        } // finalizer to release it

    // use it to write file contents
    fileResource use { writer => IO(writer.write(contents)) }
  }

  def downloadFileChunk(partId:     Nat,
                        latch:      CountDownLatch[IO],
                        filename:   String,
                        destFolder: String): IO[Unit] =
    for {
      _     <- IO(s"[downloading part $partId]").myDebug
      chunk <- FileServer.getFileChunk(partId) // Download file part
      _     <- writeToFile(s"$destFolder/$filename.$partId", chunk) // e.g. "src/main/resources/foo.1"
      _     <- IO(s"[done downloading part $partId]").myDebug
      _     <- latch.release
    } yield ()

  //===========================================================================
  // Methods related to STITCHING file together

  /**
   * Appends the file contents from the file at fromPath to the file at
   * toPath
   */
  def appendFileContents(fromPath: String, toPath: String): IO[Unit] = {
    // (I added the type of compositeResource for my own edification; the compiler infers it.)
    val compositeResource: Resource[IO, (BufferedSource, FileWriter)] =
      for {
        // resources for both reader and writer
        reader <- Resource.make { IO(Source.fromFile(fromPath)) }
                                { source => IO(source.close()) }
        writer <- Resource.make { IO(new FileWriter(new File(toPath), true)) }  // set append = true
                                {writer => IO(writer.close())}
      } yield (reader, writer)

    // Use the resource to read from the 'from' file and append to the 'to' file
    compositeResource use {
      case (reader, writer) => IO(reader.getLines().foreach(writer.write))
    }
  }

  //---------------------------------------------------------------------------
  def stitchFile(n:               Nat,    // e.g. 3
                 filename:        String,
                 destFolder:      String): IO[Unit] = {

    val toPath = s"$destFolder/$filename" // e.g. "src/main/resources/bar.txt"

    def stitchFileHelper(partId: Nat): IO[Unit] = {
      val filePartName  = s"$destFolder/$filename.$partId"  // e.g. "src/main/resources/foo.1"
      for {
        _ <- IO(s"Appending file part [$filePartName] to [$toPath]").myDebug
        _ <- appendFileContents(filePartName, toPath)
        _ <- if partId >= (n-1) then IO.unit else stitchFileHelper(partId + 1)
      } yield ()
    }

    stitchFileHelper(0)
  }

  //---------------------------------------------------------------------------
  def createFileStitcher(filename:    String, // e.g. "bar.txt"
                         destFolder:  String, // e.g. "src/main/resources"
                         latch:       CountDownLatch[IO]): IO[Unit] = for {
    _ <- IO(s"[file stitcher - step 1] waiting for file download to complete...").myDebug
    _ <- latch.await // blocks this fiber until the latch count reaches 0
    _ <- IO(s"[file stitcher - step 2] About to stitch file").myDebug

    // Stitch the file together from its parts
    n <- FileServer.getNumChunks
    _ <- stitchFile(n, filename, destFolder)

    _ <- IO(s"[file stitcher - step 3] Done stitching file: $filename").myDebug
  } yield ()

  //===========================================================================
  /**
   * This method is the target of the exercise. Here's my solution.
   * (The instructor's solution is better in many ways; it's in downloadFile_v2
   * later in this file.)
   *
   * - call file server api and get the number of chunks (n)
   * - start a CD latch with n count
   * - start n fibers that download a chunk of the file using the file server's download chunk api
   * - each task also writes the chunk to a temporary file on disk
   * - block on the latch until each task has finished
   * - after all chunks are done, stitch the files together under the same file on disk
   *   by calling the appendFileContents auxiliary method.
   */
  def downloadFile(filename: String, destFolder: String): IO[Unit] = for {
    n             <- FileServer.getNumChunks
    latch         <- CountDownLatch[IO](n)
    fibStitcher   <- createFileStitcher(filename, destFolder, latch).start
    _             <- (0 until n).toList parTraverse (downloadFileChunk(_, latch, filename, destFolder))
    _             <- fibStitcher.join
    _             <- IO("Joined file stitcher fiber. All done").myDebug
  } yield ()

  /**
   * Output from calling
   * downloadFile(filename = "myScalaFile.txt", destFolder = "src/main/resources")
   *
   [io-compute-2] [file stitcher - step 1] waiting for file download to complete...
   [io-compute-2] [downloading part 0]
   [io-compute-0] [downloading part 1]
   [io-compute-3] [downloading part 2]
   [io-compute-0] [done downloading part 1]
   [io-compute-2] [done downloading part 0]
   [io-compute-3] [done downloading part 2]
   [io-compute-0] [file stitcher - step 2] About to stitch file
   [io-compute-0] Appending file part [src/main/resources/myScalaFile.txt.0] to [src/main/resources/myScalaFile.txt]
   [io-compute-0] Appending file part [src/main/resources/myScalaFile.txt.1] to [src/main/resources/myScalaFile.txt]
   [io-compute-0] Appending file part [src/main/resources/myScalaFile.txt.2] to [src/main/resources/myScalaFile.txt]
   [io-compute-0] [file stitcher - step 3] Done stitching file: myScalaFile.txt
   [io-compute-0] Joined file stitcher fiber. All done
   */

  //===========================================================================
  // INSTRUCTOR'S SOLUTION

  def createFileDownloaderTask (id:         Nat,
                                latch:      CountDownLatch[IO],
                                filename:   String,
                                destFolder: String): IO[Unit] =
    for {
      _     <- IO(s"[task $id] downloading chunk...").myDebug
            // sleep up to a second to avoid contention at the file server
      _     <- IO sleep (Random.nextDouble * 1000).toInt.millis
      chunk <- FileServer.getFileChunk(id)
      _     <- writeToFile(s"$destFolder/$filename.$id", chunk)
      _     <- IO(s"[task $id] chunk download complete").myDebug
      _     <- latch.release
    } yield ()

  //---------------------------------------------------------------------------
  /**
   * Instructor's solution is better than mine in the following ways.
   *
   * 1. Uses traverse to stitch together the file parts, so no need to create a
   *    tail-recursive method to do it.
   * 2. Flow is much better: starts the download first and then calls latch.await()
   *    instead of calling the file stitcher first and making it call latch.await
   *    to wait on the latch.
   * 3. Code is much more compact, with just one additional helper method,
   *    createFileDownloaderTask.
   * 4. Counts the parts from 0 to n-1 instead of from 1 to n.
   * 5. Each download task sleeps for upto 1 second to avoid contention at the
   *    file server.
   */
  def downloadFile_v2(filename: String, destFolder: String): IO[Unit] =
    for {
      n     <- FileServer.getNumChunks
      latch <- CountDownLatch[IO](n)
      _     <- IO(s"Download started on $n fibers").myDebug
      _     <- (0 until n).toList.parTraverse (createFileDownloaderTask(_, latch, filename, destFolder))
      _     <- latch.await  // wait for all the downloads to complete
      _     <- (0 until n).toList.traverse { id =>
                          appendFileContents(fromPath = s"$destFolder/$filename.$id",
                                             toPath   = s"$destFolder/$filename") }
      _     <- IO("File has been stitched together").myDebug
    } yield ()

  /**
   * Run:
   * downloadFile_v2(filename = "myScalaFile.txt", destFolder = "src/main/resources")
   *
   * Output:
   * [io-compute-0] Download started on 3 fibers
   * [io-compute-3] [task 0] downloading chunk...
   * [io-compute-0] [task 1] downloading chunk...
   * [io-compute-2] [task 2] downloading chunk...
   * [io-compute-0] [task 1] chunk download complete
   * [io-compute-2] [task 2] chunk download complete
   * [io-compute-3] [task 0] chunk download complete
   * [io-compute-3] File has been stitched together
   */

  //---------------------------------------------------------------------------
  override def run: IO[Unit] = downloadFile_v2(filename = "myScalaFile.txt", destFolder = "src/main/resources")
}
