package cs451;

import java.util.ArrayList;
import java.util.List;

public class PositiveRangeSet {

    int min = 0;
    List<Range> outliers;

    public PositiveRangeSet() {
        outliers = new ArrayList<>();
        min = 0;
    }

    public void push(int i) {
        if (this.contains(i)) {
            return;
        }
        if (i == min + 1) {
            min++;
            for (int j = 0; j < outliers.size(); ++j) {
                Range r = outliers.get(j);
                if (r.start == min + 1) {
                    min = r.end;
                    outliers.remove(j);
                    return;
                }
            }
        } else {
            Range previous = null;
            Range next = null;
            for (int j = 0; j < outliers.size(); ++j) {
                Range r = outliers.get(j);
                if (r.end == i - 1) {
                    r.end++;
                    if (next != null) {
                        next.start = r.start;
                        outliers.remove(j);
                        return;
                    }
                    previous = r;
                } else if (r.start == i + 1) {
                    r.start--;
                    if (previous != null) {
                        previous.end = r.end;
                        outliers.remove(j);
                        return;
                    }
                    next = r;
                }
            }
        }
    }

    public boolean contains(int i) {
        for (var r : outliers) {
            if (r.start <= i && i <= r.end) {
                return true;
            }
        }
        return false;
    }

    private class Range {
        int start;
        int end;

        public Range(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }
}
