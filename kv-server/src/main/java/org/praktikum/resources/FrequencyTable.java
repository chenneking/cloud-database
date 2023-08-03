package org.praktikum.resources;

public class FrequencyTable {
    // die number of buckets darf nicht final sein, es kann ja sein, dass wir von z.B 5 buckets 2 abgeben, und dann nur noch 3 haben!
    private int numberOfBuckets;
    private int offloadThreshold;

    // Nimm hier am besten eine Liste (da unsere anzahl an buckets nicht statisch ist) die sorted ist (also vllcht. sowas wie ne arraylist?)

    public FrequencyTable(int numberOfBuckets, int offloadThreshold) {
        this.numberOfBuckets = numberOfBuckets;
        //TODO Exception werfen wenn offloadThreshhold < 0 oder > 100
        this.offloadThreshold = offloadThreshold;
    }

    // TODO: methode nimmt jegliche operation an (ob wir alle puts, deletes und gets loggen ist noch unklar) und sortiert den dann in die frequency table ein
    public void addOperation(String type, String hash) {

    }

    // TODO: Methode die basierend auf der Frequency table berechnet welche Keyrange abgegeben werden soll
    // lower: von links, nicht lower: von rechts
    public String[] calculateOffloadKeyrange(boolean lower) {
        return null;
    }

    // TODO: this method should return ascii string of pretty-printed frequency table that can be sent to kv-client
    @Override
    public String toString() {
        return "";
    }
}
