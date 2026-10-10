#include <stdio.h>
#include "rrc_glob.h"
static int fails = 0;
#define T(expect, pat, path) do { bool r = rrc_glob_match(pat, path); if (r != (expect)) { fails++; printf("FAIL glob('%s','%s') = %d\n", pat, path, r); } } while (0)
int main(void) {
    T(true, "*.nef", "DCIM/100NIKON/DSC_0001.NEF");
    T(true, "*.NEF", "dsc_0001.nef");
    T(false, "*.nef", "DCIM/100NIKON/DSC_0001.NEF.xmp");
    T(true, "DSC_????.NEF", "DSC_0001.NEF");
    T(false, "DSC_????.NEF", "DSC_00001.NEF");
    T(true, "DCIM/*/*.NEF", "DCIM/100NIKON/DSC_0001.NEF");
    T(false, "DCIM/*/*.NEF", "DCIM/100NIKON/sub/DSC_0001.NEF");
    T(true, "DCIM/**/*.NEF", "DCIM/100NIKON/sub/DSC_0001.NEF");
    T(true, "DCIM/**/*.NEF", "DCIM/DSC_0001.NEF");
    T(true, "**/*.jpg", "a/b/c/x.JPG");
    T(true, "**/*.jpg", "x.JPG");
    T(true, "*.[jJ][pP][gG]", "x.jpg");
    T(true, "*.[a-d]ng", "x.DNG");
    T(false, "*.[!d]ng", "x.DNG");
    T(true, "*.[!d]ng", "x.PNG");
    T(true, "/DCIM/*.NEF", "/DCIM/a.nef");
    T(false, "", "x");
    T(true, "*", "anything.ext");
    T(false, "a*b", "a/b");
    T(true, "a\\*b", "A*B");
    T(false, "*.jpg", "DCIM/100NIKON/DSC_0001.NEF");
    T(true, "DCIM/100NIKON/DSC_0001.NEF", "DCIM/100NIKON/DSC_0001.NEF");
    if (!rrc_glob_match_any("*.nef, *.dng;*.jpg *.jpeg", "x/y.DNG")) { fails++; printf("FAIL any1\n"); }
    if (rrc_glob_match_any("*.nef, *.dng", "x/y.mov")) { fails++; printf("FAIL any2\n"); }
    if (rrc_glob_match_any("", "x/y.mov")) { fails++; printf("FAIL any empty\n"); }
    if (!rrc_glob_selected("*.nef *.jpg", "*_small.jpg", "a/b.jpg")) { fails++; printf("FAIL sel1\n"); }
    if (rrc_glob_selected("*.nef *.jpg", "*_small.jpg", "a/b_small.jpg")) { fails++; printf("FAIL sel2\n"); }
    if (!rrc_glob_selected("*.nef", NULL, "a/b.nef")) { fails++; printf("FAIL sel3\n"); }
    printf(fails ? "FAILED (%d)\n" : "ALL OK\n", fails);
    return fails ? 1 : 0;
}
