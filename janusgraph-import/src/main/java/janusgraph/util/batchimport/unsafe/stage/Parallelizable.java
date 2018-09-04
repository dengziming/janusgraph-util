package janusgraph.util.batchimport.unsafe.stage;

/**
 * Represents something that can be parallelizable, in this case that means the ability to dynamically change
 * the number of processors executing tasks.
 */
public interface Parallelizable
{
    /**
     * Change number of processors assigned to this {@link Parallelizable}. Accepts a {@code delta},
     * which may specify positive or negative value, even zero. This instances may have internal constraints
     * in the number of processors, min or max, which may be assigned and so potentially the change will
     * only be partially accepted or not at all. This is why this call returns the total number of processors
     * this instance now has accepted after any effect of this call.
     *
     * {@link Parallelizable} is used in many call stacks where call delegation is predominant and so
     * reducing number of methods to delegate is favored. This is why this method looks and functions
     * like this, it can cater for incrementing, decrementing and even getting number of processors.
     *
     * @param delta number of processors to add or remove, i.e. negative or positive value. A value of
     * zero will result in merely the current number of assigned processors to be returned.
     * @return the number of assigned processors as a result this call.
     */
    default int processors(int delta)
    {
        return 1;
    }
}
