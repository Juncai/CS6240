package utils;

import org.apache.commons.lang.time.DateUtils;

import java.util.*;

/**
 * Created by Jun Cai on 2/14/16.
 */

public class ConnectionInfo {
    private List<Date> arrTSs;
    private List<Date> depTSs;
    private List<Date[]> coverRanges;
    private boolean crMerged; // cover range merged flag
    private boolean arrFiltered; // arrive flights filtered flag

    public ConnectionInfo() {
        arrTSs = new ArrayList<Date>();
        depTSs = new ArrayList<Date>();
        coverRanges = new ArrayList<Date[]>();
        crMerged = false;
        arrFiltered = false;
    }

    public void updateArr(Date ts) {
        arrTSs.add(ts);
    }

    public void updateDep(Date ts) {
        depTSs.add(ts);
    }

    public void updateCoverRanges(Date[] cover) {
        crMerged = true;
        coverRanges.add(cover);
    }

    public void processCoverRangesInReducer() {
        if (coverRanges.size() == 0) return;
        Collections.sort(coverRanges, new Comparator<Date[]>() {
            @Override
            public int compare(Date[] d1, Date[] d2) {
                if (d1[0].getTime() < d2[0].getTime()) {
                    return -1;
                } else if (d1[0].getTime() == d2[0].getTime()) {
                    return 0;
                } else {
                    return 1;
                }
            }
        });

        // merge the cover ranges to a new list, then replace the old one
        List<Date[]> newCR = new ArrayList<Date[]>();
        Date[] newCover = coverRanges.get(0);
        newCR.add(newCover);
        Date[] cCover;
        for (int i = 1; i < coverRanges.size(); i++) {
            cCover = coverRanges.get(i);
            if (cCover[0].getTime() <= newCover[1].getTime()) {
                newCover[1] = cCover[1];
            } else {
                newCR.add(cCover);
                newCover = cCover;
            }
        }
        coverRanges = newCR;
    }

    public List<Date> getArrTSs() {
        filterArr();
        return arrTSs;
    }

    public List<Date> getRawArrTSs() {
        return arrTSs;
    }

    public List<Date[]> getCoverRanges() {
        mergeCoverRanges();
        return coverRanges;
    }

    public void mergeCoverRanges() {
        if (crMerged) return;
        Date[] cover = null;
        Date coverStart;
        Date coverEnd;

        Collections.sort(arrTSs);
        Collections.sort(depTSs);

        // first combine depTSs to coverRange
        for (int i = 0; i < depTSs.size(); i++) {
            coverStart = DateUtils.addHours(depTSs.get(i), -6);
            coverEnd = DateUtils.addMinutes(depTSs.get(i), -30);
            if (cover == null || coverStart.getTime() > cover[1].getTime()) {
                cover = new Date[2];
                cover[0] = coverStart;
                cover[1] = coverEnd;
                coverRanges.add(cover);
            } else {
                cover[1] = coverEnd;
            }
        }

        crMerged = true;
    }

    public void filterArr() {
        if (arrFiltered) return;

        mergeCoverRanges();

        if (arrTSs.size() == 0 || coverRanges.size() == 0) return;

        List<Date> coveredArrs = new ArrayList<Date>();

        // filter out the flights which is already covered
        int arrInd = 0;
        int coverInd = 0;
        Date cArr = arrTSs.get(arrInd);
        Date[] cCover = coverRanges.get(coverInd);

        while (arrInd < arrTSs.size()) {
            if (cArr.getTime() < cCover[0].getTime()) {
                if (++arrInd < arrTSs.size()) {
                    cArr = arrTSs.get(arrInd);
                }
            } else if (cArr.getTime() <= cCover[1].getTime()) {
                coveredArrs.add(cArr);
                if (++arrInd < arrTSs.size()) {
                    cArr = arrTSs.get(arrInd);
                }
            } else if (++coverInd < coverRanges.size()) {
                    cCover = coverRanges.get(coverInd);
            } else {
                break;
            }
        }

        for (Date ca : coveredArrs) {
            arrTSs.remove(ca);
        }

        arrFiltered = true;
    }
}
