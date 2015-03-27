package org.apache.kylin.storage.gridtable;

import java.util.Collections;
import java.util.List;

public class GTScanRange {

    final public GTRecord pkStart; // inclusive
    final public GTRecord pkEnd; // inclusive
    final public List<GTRecord> hbaseFuzzyKeys; // partial matching primary keys

    public GTScanRange(GTRecord pkStart, GTRecord pkEnd) {
        this(pkStart, pkEnd, null);
    }

    public GTScanRange(GTRecord pkStart, GTRecord pkEnd, List<GTRecord> hbaseFuzzyKeys) {
        this.pkStart = pkStart;
        this.pkEnd = pkEnd;
        this.hbaseFuzzyKeys = hbaseFuzzyKeys == null ? Collections.<GTRecord> emptyList() : hbaseFuzzyKeys;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hbaseFuzzyKeys == null) ? 0 : hbaseFuzzyKeys.hashCode());
        result = prime * result + ((pkEnd == null) ? 0 : pkEnd.hashCode());
        result = prime * result + ((pkStart == null) ? 0 : pkStart.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GTScanRange other = (GTScanRange) obj;
        if (hbaseFuzzyKeys == null) {
            if (other.hbaseFuzzyKeys != null)
                return false;
        } else if (!hbaseFuzzyKeys.equals(other.hbaseFuzzyKeys))
            return false;
        if (pkEnd == null) {
            if (other.pkEnd != null)
                return false;
        } else if (!pkEnd.equals(other.pkEnd))
            return false;
        if (pkStart == null) {
            if (other.pkStart != null)
                return false;
        } else if (!pkStart.equals(other.pkStart))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return (pkStart == null ? "null" : pkStart.toString(pkStart.info.primaryKey)) //
                + "-" + (pkEnd == null ? "null" : pkEnd.toString(pkEnd.info.primaryKey));
    }
}
