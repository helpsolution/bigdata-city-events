package types;

import com.google.common.base.Objects;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogWritable implements WritableComparable<LogWritable> {

    private Text type, name;
    private IntWritable value;

    public LogWritable() {
        type = new Text();
        name = new Text();
        value = new IntWritable();
    }

    public LogWritable(Text type, Text name) {
        this.type = type;
        this.name = name;
    }

    public LogWritable(Text type, IntWritable value) {
        this.type = type;
        this.value = value;
    }

    public LogWritable(Text type, Text name, IntWritable value) {
        this.type = type;
        this.name = name;
        this.value = value;
    }


    public void set(Text id, Text name, IntWritable value) {
        this.type = id;
        this.name = name;
        this.value = value;
    }

    public void setType(Text type) {
        this.type = type;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public void setValue(IntWritable value) {
        this.value = value;
    }

    public Text getType() {
        return this.type;
    }

    public Text getName() {
        return this.name;
    }

    public IntWritable getValue() {
        return this.value;
    }

    public int compareTo(LogWritable o) {
        Text thisV = this.name;
        Text anotherV = o.name;
        return thisV.compareTo(anotherV);
    }

    public void write(DataOutput dataOutput) throws IOException {
        type.write(dataOutput);
        name.write(dataOutput);
        value.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        type.readFields(dataInput);
        name.readFields(dataInput);
        value.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogWritable that = (LogWritable) o;
        return Objects.equal(type, that.type) &&
                Objects.equal(name, that.name) &&
                Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type, name, value);
    }

    @Override
    public String toString() {
        return "LogWritable{" +
                "type=" + type +
                ", name=" + name +
                ", value=" + value +
                '}';
    }
}
