// The C++17 wrapper must round-trip the fixtures through std:: types.
#include <fstream>
#include <iostream>
#include <sstream>
#include "rrcloud_proto.hpp"
#include "rrc_blake3.h"

extern "C" void rrcp_blake3_hook(const uint8_t *data, size_t len, uint8_t out[32]) { rrc_blake3(data, len, out); }

static int fails = 0;
#define CHECK(c, msg) do { if (!(c)) { fails++; std::cout << "FAIL " << __LINE__ << ": " << msg << "\n"; } } while (0)

int main(int argc, char **argv)
{
    using namespace rrcloud::proto;
    std::string dir = argc > 1 ? argv[1] : "../../fixtures";
    std::ifstream f(dir + "/journal_segment.v1.ndjson");
    std::string line;
    int n = 0;
    while (std::getline(f, line)) {
        auto e = JournalEntry::from_json(line);
        CHECK(e.to_json() == line, "round trip line " << n);
        if (n == 1) { CHECK(e.rating && *e.rating == 3, "rating"); CHECK(e.vv.size() == 2, "vv size"); CHECK(e.color_label && *e.color_label == "red", "color"); }
        n++;
    }
    CHECK(n == 5, "line count");
    try { JournalEntry::from_json(R"({"v":9,"seq":1,"ts":0,"device":"0f6b2a1e-1111-4222-8333-944444444444","op":"put","kind":"original","key":"k","vv":{}})"); CHECK(false, "no throw"); }
    catch (const Error &err) { CHECK(err.code == RRCP_E_UNSUPPORTED_VERSION, "version gate code"); }
    std::ifstream t(dir + "/tombstone.json");
    std::getline(t, line);
    auto ts = Tombstone::from_json(line);
    CHECK(ts.kinds.size() == 3 && ts.kinds[0] == Kind::Original, "kinds");
    CHECK(ts.to_json() == line, "tombstone round trip");
    CHECK(key_manifest("0f6b2a1e-1111-4222-8333-944444444444") == ".rrcloud/v1/manifests/0f6b2a1e-1111-4222-8333-944444444444.json.gz", "key");
    CHECK(key_thumb("6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f", ThumbSize::Small).find("_small.jpg") != std::string::npos, "thumb key");
    CHECK(std::string(to_wire(Op::Attest)) == "attest", "enum wire");
    CHECK(JOURNAL_VERSION == 1 && LIBRARY_PREFIX == "library/", "constants");
    DeviceEntry de; de.name = "x"; de.platform = "esp32"; de.created = 1; de.last_seen_server_ts = 2; de.proto.read = {1}; de.proto.write = 1; de.applied["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"] = 7;
    CHECK(de.to_json() == R"({"name":"x","platform":"esp32","created":1,"last_seen_server_ts":2,"applied":{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa":7},"proto":{"read":[1],"write":1}})", "device entry encode " << de.to_json());
    std::cout << (fails ? "FAILED" : "ALL OK") << "\n";
    return fails ? 1 : 0;
}
