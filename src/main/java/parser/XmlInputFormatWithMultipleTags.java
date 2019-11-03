package parser;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * An implementation of XML input format.
 * Referred https://github.com/Mohammed-siddiq/hadoop-XMLInputFormatWithMultipleTags,
 * and added support for another tag.
 */
public class XmlInputFormatWithMultipleTags extends TextInputFormat {

    public static final String START_TAG_KEYS = "xmlinput.start";
    public static final String END_TAG_KEYS = "xmlinput.end";


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) {
        return new XmlRecordReader();
    }

    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        //execution starts here, create array of bytes for each start and end tags
        private byte[][] startTags;
        private byte[][] endTags;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();


        @Override
        public void initialize(InputSplit is, TaskAttemptContext ctx) throws IOException {
            FileSplit fSplit = (FileSplit) is;

            String[] sTags = ctx.getConfiguration().get(START_TAG_KEYS).split(",");
            String[] eTags = ctx.getConfiguration().get(END_TAG_KEYS).split(",");

            startTags = new byte[sTags.length][];
            endTags = new byte[sTags.length][];
            for (int i = 0; i < sTags.length; i++) {
                startTags[i] = sTags[i].getBytes(StandardCharsets.UTF_8);
                endTags[i] = eTags[i].getBytes(StandardCharsets.UTF_8);
            }

            start = fSplit.getStart();
            end = start + fSplit.getLength();

            Path file = fSplit.getPath();
            FileSystem sys = file.getFileSystem(ctx.getConfiguration());
            fsin = sys.open(fSplit.getPath());
            fsin.seek(start);

        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (fsin.getPos() < end) {
                //Changed here to perform readuntillmatch to all the tags
                int res = readUntilMatch(startTags, false);
                if (res != -1) { // Read until start_tag1 or start_tag2
                    try {

                        buffer.write(startTags[res - 1]);
                        //Changed to read all the contents before the end tag
                        int res1 = readUntilMatch(endTags, true);
                        if (res1 != -1) { // changed here
                            // updating the buffer with contents between start and end tags.
                            value.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        private int readUntilMatch(byte[][] match, boolean withinBlock) throws IOException {
            int tag1= 0, tag2= 0, tag3= 0, tag4= 0, tag5= 0, tag6= 0, tag7= 0, tag8 = 0;
            while (true) {
                int b = fsin.read();

                if (b == -1) {
                    return -1;// to check the end of file
                }

                if (withinBlock) {
                    buffer.write(b);// save to buffer:
                }

                if (b == match[0][tag1]) {                 // check if there's any matching tags

                    tag1++;
                    if (tag1 >= match[0].length) return 1;
                } else tag1 = 0;

                if (b == match[1][tag2]) {
                    tag2++;
                    if (tag2 >= match[1].length) return 2;
                } else tag2 = 0;

                if (b == match[2][tag3]) {
                    tag3++;
                    if (tag3 >= match[2].length) return 3;
                } else tag3 = 0;

                if (b == match[3][tag4]) {
                    tag4++;
                    if (tag4 >= match[3].length) return 4;
                } else tag4 = 0;

                if (b == match[4][tag5]) {
                    tag5++;
                    if (tag5 >= match[4].length) return 5;
                } else tag5 = 0;

                if (b == match[5][tag6]) {
                    tag6++;
                    if (tag6 >= match[5].length) return 6;
                } else tag6 = 0;

                if (b == match[6][tag7]) {
                    tag7++;
                    if (tag7 >= match[6].length) return 7;
                } else tag7 = 0;

                if (b == match[7][tag8]) {
                    tag8++;
                    if (tag8 >= match[7].length) return 8;
                } else tag8 = 0;

                // see if we've passed the stop point:
                if (!withinBlock && (tag1 == 0 && tag2 == 0 && tag3 == 0 && tag4 == 0 && tag5 == 0 && tag6 == 0 && tag7 == 0) && tag8 == 0 && fsin.getPos() >= end)
                    return -1;
            }
        }

    }

}
