import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import types.LogWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HandlerTest {
    MapDriver<Object, Text, Text, LogWritable> logMapDriver;
    MapDriver<Object, Text, Text, LogWritable> cityMapDriver;
    ReduceDriver<Text, LogWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, LogWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Handler.LogsMapper logMapper = new Handler.LogsMapper();
        Handler.CityInfoMapper cityInfoMapper = new Handler.CityInfoMapper();
        Handler.ReduceJoinReducer reducer = new Handler.ReduceJoinReducer();
        logMapDriver = MapDriver.newMapDriver(logMapper);
        cityMapDriver =  MapDriver.newMapDriver(cityInfoMapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testLogMapper() throws IOException {
        logMapDriver.withInput(new LongWritable(), new Text("9c40b049d4c7d584393d08195471b7e\t20131022194800572\t1\tC59HWECf1T\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; znwb6800)\t123.149.223.*\t164\t165\t2\tc9831086e852f8ef6a94b56ab78a371b\ta1226ece0fdab33c3f30746a8bad6356\tnull\t1790983176\t336\t280\tOtherView\tNa\t5\t10720\t277\t110\tnull\t2821\t10057,13800,10076,10075,13042,10129,13866,10024,10006,10110,10031,10126,10145,10114,13403,10063"));
        logMapDriver.withInput(new LongWritable(), new Text("9c40b049d4c7d584393d08195471b7e\t20131022194800572\t1\tC59HWECf1T\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; znwb6800)\t123.149.223.*\t164\t999\t2\tc9831086e852f8ef6a94b56ab78a371b\ta1226ece0fdab33c3f30746a8bad6356\tnull\t1790983176\t336\t280\tOtherView\tNa\t5\t10720\t290\t110\tnull\t2821\t10057,13800,10076,10075,13042,10129,13866,10024,10006,10110,10031,10126,10145,10114,13403,10063"));
        logMapDriver.withOutput(new Text("165"), new LogWritable(new Text("value"),new Text(), new IntWritable(1)));
        logMapDriver.withOutput(new Text("999"), new LogWritable(new Text("value"),new Text(), new IntWritable(1)));
        logMapDriver.runTest();
    }

    @Test
    public void testSityInfoMapper() throws IOException {
        cityMapDriver.withInput(new LongWritable(), new Text("165\tshijiazhuang"));
        cityMapDriver.withInput(new LongWritable(), new Text("999\ttangshan"));

        cityMapDriver.withOutput(new Text("165"), new LogWritable(new Text("name"),new Text("shijiazhuang"), new IntWritable()));
        cityMapDriver.withOutput(new Text("999"), new LogWritable(new Text("name"),new Text("tangshan"), new IntWritable()));
        cityMapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<LogWritable> values1 = new ArrayList<LogWritable>();
        values1.add(new LogWritable(new Text("value"),new Text(), new IntWritable(1)));
        values1.add(new LogWritable(new Text("name"),new Text("shijiazhuang"), new IntWritable()));
        values1.add(new LogWritable(new Text("value"),new Text(), new IntWritable(1)));
        reduceDriver.withInput(new Text("165"), values1);

        List<LogWritable> values2 = new ArrayList<LogWritable>();
        values2.add(new LogWritable(new Text("value"),new Text(), new IntWritable(1)));
        values2.add(new LogWritable(new Text("name"),new Text("tangshan"), new IntWritable()));
        values2.add(new LogWritable(new Text("value"),new Text(), new IntWritable(1)));
        values2.add(new LogWritable(new Text("value"),new Text(), new IntWritable(1)));
        reduceDriver.withInput(new Text("999"), values2);


        reduceDriver.withOutput(new Text("shijiazhuang"), new IntWritable(2));
        reduceDriver.withOutput(new Text("tangshan"), new IntWritable(3));

        reduceDriver.runTest();
    }

}