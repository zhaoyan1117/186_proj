package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] histogram;
    private int min;
    private int max;
    private int width;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        this.width = (int)Math.ceil ( (max - min + 1) / (double)buckets );
        this.ntups = 0;
        this.histogram = new int[buckets];
    }

    private int getBucket(int v) {
        return Math.max((v - this.min), 0) / width;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucket = this.getBucket(v);
        this.histogram[bucket]++;
        this.ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        switch (op) {
            case EQUALS:
                if (v > this.max || v < this.min) { return 0.0; }
                return equalsSelectivity(v);
            case NOT_EQUALS:
                if (v > this.max || v < this.min) { return 1.0; }
                return 1.0 - equalsSelectivity(v);
            case GREATER_THAN:
                if (v < this.min) { return 1.0; }
                if (v >= this.max) { return 0.0; }
                return greaterThanSelectivity(v);
            case LESS_THAN_OR_EQ:
                if (v < min) { return 0.0; }
                if (v > max) { return 1.0; }
                return 1.0 - greaterThanSelectivity(v);
            case LESS_THAN:
                if (v <= this.min) { return 0.0; }
                if (v > this.max) { return 1.0; }
                return 1.0 - greaterThanOrEqualSelectivity(v);
            case GREATER_THAN_OR_EQ:
                if (v < min) { return 1.0; }
                if (v > max) { return 0.0; }
                return greaterThanOrEqualSelectivity(v);
        }

        // Should never reach here.
        return -1.0;
    }

    private double equalsSelectivity(int v) {
        int bucket = this.getBucket(v);
        return ((double) this.histogram[bucket] / this.width) / this.ntups;
    }

    private double greaterThanSelectivity(int v) {
        int bucket = this.getBucket(v);
        double b_f = (double) this.histogram[bucket] / this.ntups;
        double b_part = (double) ((this.width * bucket) - (v - this.min)) / this.width;
        
        double selectivity = b_f * b_part;
        for (int i = bucket + 1; i < this.histogram.length; i++) {
            selectivity += (double) this.histogram[i] / this.ntups;
        }

        return selectivity;
    }

    private double greaterThanOrEqualSelectivity(int v) {
        return equalsSelectivity(v) + greaterThanSelectivity(v);
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String buckets = "[ ";
        for(int i = 0; i < histogram.length-1; i++){
            buckets += this.histogram[i] + " | ";
        }
        buckets += this.histogram[histogram.length-1] + " ]";

        String result = "min: " + this.min + "\n" +
                        "max: " + this.max + "\n" +
                        "width: " + this.width + "\n" +
                        "ntups: " + this.ntups + "\n" +
                        "histogram: " + buckets;
        return result;
    }
}
