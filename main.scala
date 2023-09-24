//> using scala "3.2.2"
//> using lib "co.fs2::fs2-io:3.6.1"
//> using lib "io.circe::circe-core:0.14.5"
//> using lib "io.circe::circe-parser:0.14.5"
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.IOApp
import cats.implicits.*
import cats.kernel.Order
import fs2.io.file.Files
import fs2.io.file.Path
import io.circe.Codec
import io.circe.Decoder

enum Event {

  case PushEvent(
    payload: PushPayload
  )

  case OtherEvent(
    typeName: String
  )

}

object Event {

  given Decoder[Event] = {
    val pushEventDecoder = Decoder.derived[PushEvent]

    Decoder[String].at("type").flatMap {
      case "PushEvent" => pushEventDecoder.widen
      case other       => Decoder.const(Event.OtherEvent(other))
    }
  }

}

case class PushPayload(
  commits: List[Commit]
) derives Codec.AsObject

case class Commit(
  message: String,
  author: Author,
) derives Codec.AsObject

case class Author(
  name: String
) derives Codec.AsObject

object Author {
  given Order[Author] = Order.by(_.name)
}

object Main extends IOApp.Simple {

  val inputPath = Path("10K.github.jsonl")
  val outputPath = Path("ngrams.csv")

  val readPayloads: IO[List[PushPayload]] =
    Files[IO]
      .readUtf8Lines(inputPath)
      .filter(_.strip.nonEmpty)
      .evalMap(io.circe.parser.decode[Event](_).liftTo[IO])
      .collect { case Event.PushEvent(payload) => payload }
      .compile
      .toList

  def groupPayloads(
    payloads: List[PushPayload]
  ): Map[Author, NonEmptyList[String]] = payloads
    .flatMap(_.commits)
    .groupByNel(_.author)
    .fmap(_.map(_.message))

  def convert(
    author: Author,
    commitMessages: NonEmptyList[String],
  ): OutputLine = {
    val allNgrams = commitMessages.map(_.toLowerCase).toList.flatMap { message =>
      val words =
        message
          .split("\\s+")
          .map { w =>
            w.filter(!Set(',', '.', '!', '?').contains_(_))
          }
          .filterNot(_.strip().isEmpty)
          .toList

      makeNGrams(words, 3)
    }

    val top5NGrams: List[NGram] = allNgrams
      .groupByNel(identity)
      .toList
      .sortBy(_._2.size)
      .map(_._1)
      .reverse
      .take(5)

    OutputLine(author.name, top5NGrams)
  }

  def makeNGrams(
    words: List[String],
    length: Int,
  ): List[NGram] =
    words
      .sliding(size = length, step = 1)
      .map(NGram(_))
      .toList

  def writeLines(
    lines: List[OutputLine]
  ): IO[Unit] = {
    val rendered =
      (
        fileHeading ::
          lines.map(renderLine)
      ).mkString("", "\n", "\n")

    fs2
      .Stream
      .emit(rendered)
      .through(fs2.text.utf8.encode[IO])
      .through(Files[IO].writeAll(outputPath))
      .compile
      .drain
  }

  val fileHeading: String = List(
    "author",
    "first 3-gram",
    "second 3-gram",
    "third 3-gram",
    "fourth 3-gram",
    "fifth 3-gram",
  ).map(_.quoted).mkString(",")

  def renderLine(
    outputLine: OutputLine
  ): String = {
    val columns: List[String] =
      outputLine.author.quoted ::
        outputLine.ngrams.map { ngram =>
          ngram.words.mkString(" ").quoted
        }

    columns.mkString(",")
  }

  extension (
    s: String
  ) def quoted = s"'$s'"

  def run: IO[Unit] = readPayloads
    .map(groupPayloads)
    .map(_.map(convert).toList)
    .flatMap(writeLines)

}

case class NGram(
  words: List[String]
)

object NGram {
  given Order[NGram] = Order.by(_.words)
}

case class OutputLine(
  author: String,
  ngrams: List[NGram],
)
