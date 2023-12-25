package cz.scholz.kekspose;

/**
 * Exception used to indicate errors
 */
public class Keksception extends RuntimeException {
    /**
     * Creates the Keksception
     *
     * @param message   Error message
     */
    public Keksception(String message)  {
        super(message);
    }
}
