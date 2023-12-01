/* Sole is a lightweight C++11 library to generate universally unique identificators.
 * Sole provides interface for UUID versions 0, 1 and 4.

 * https://github.com/r-lyeh/sole
 * Copyright (c) 2013,2014,2015 r-lyeh. zlib/libpng licensed.

 * Based on code by Dmitri Bouianov, Philip O'Toole, Poco C++ libraries and anonymous
 * code found on the net. Thanks guys!

 * Theory: (see Hoylen's answer at [1])
 * - UUID version 1 (48-bit MAC address + 60-bit clock with a resolution of 100ns)
 *   Clock wraps in 3603 A.D.
 *   Up to 10000000 UUIDs per second.
 *   MAC address revealed.
 *
 * - UUID Version 4 (122-bits of randomness)
 *   See [2] or other analysis that describe how very unlikely a duplicate is.
 *
 * - Use v1 if you need to sort or classify UUIDs per machine.
 *   Use v1 if you are worried about leaving it up to probabilities (e.g. your are the
 *   type of person worried about the earth getting destroyed by a large asteroid in your
 *   lifetime). Just use a v1 and it is guaranteed to be unique till 3603 AD.
 *
 * - Use v4 if you are worried about security issues and determinism. That is because
 *   v1 UUIDs reveal the MAC address of the machine it was generated on and they can be
 *   predictable. Use v4 if you need more than 10 million uuids per second, or if your
 *   application wants to live past 3603 A.D.

 * Additionally a custom UUID v0 is provided:
 * - 16-bit PID + 48-bit MAC address + 60-bit clock with a resolution of 100ns since Unix epoch
 * - Format is EPOCH_LOW-EPOCH_MID-VERSION(0)|EPOCH_HI-PID-MAC
 * - Clock wraps in 3991 A.D.
 * - Up to 10000000 UUIDs per second.
 * - MAC address and PID revealed.

 * References:
 * - [1] http://stackoverflow.com/questions/1155008/how-unique-is-uuid
 * - [2] http://en.wikipedia.org/wiki/UUID#Random%5FUUID%5Fprobability%5Fof%5Fduplicates
 * - http://en.wikipedia.org/wiki/Universally_unique_identifier
 * - http://en.cppreference.com/w/cpp/numeric/random/random_device
 * - http://www.itu.int/ITU-T/asn1/uuid.html f81d4fae-7dec-11d0-a765-00a0c91e6bf6

 * - rlyeh ~~ listening to Hedon Cries / Until The Sun Goes up
 */

//////////////////////////////////////////////////////////////////////////////////////

#pragma once
#include <stdint.h>
#include <stdio.h>     // for size_t; should be stddef.h instead; however, clang+archlinux fails when compiling it (@Travis-Ci)
#include <sys/types.h> // for uint32_t; should be stdint.h instead; however, GCC 5 on OSX fails when compiling it (See issue #11)
#include <functional>
#include <string>

// public API

#define SOLE_VERSION "1.0.5" /* (2022/07/15): Fix #42 (clang: __thread vs threadlocal)
#define SOLE_VERSION "1.0.4" // (2022/04/09): Fix potential threaded issues (fix #18, PR #39) and a socket leak (fix #38)
#define SOLE_VERSION "1.0.3" // (2022/01/17): Merge fixes by @jasonwinterpixel(emscripten) + @jj-tetraquark(get_any_mac)
#define SOLE_VERSION "1.0.2" // (2021/03/16): Merge speed improvements by @vihangm
#define SOLE_VERSION "1.0.1" // (2017/05/16): Improve UUID4 and base62 performance; fix warnings
#define SOLE_VERSION "1.0.0" // (2016/02/03): Initial semver adherence; Switch to header-only; Remove warnings */

namespace sole
{
    // 128-bit basic UUID type that allows comparison and sorting.
    // Use .str() for printing and .pretty() for pretty printing.
    // Also, ostream friendly.
    struct uuid
    {
        uint64_t ab;
        uint64_t cd;

        bool operator==( const uuid &other ) const;
        bool operator!=( const uuid &other ) const;
        bool operator <( const uuid &other ) const;

        std::string pretty() const;
        std::string base62() const;
        std::string str() const;

        template<typename ostream>
        inline friend ostream &operator<<( ostream &os, const uuid &self ) {
            return os << self.str(), os;
        }
    };

    // Generators
    uuid uuid0(); // UUID v0, pro: unique; cons: MAC revealed, pid revealed, predictable.
    uuid uuid1(); // UUID v1, pro: unique; cons: MAC revealed, predictable.
    uuid uuid4(); // UUID v4, pros: anonymous, fast; con: uuids "can clash"

    // Rebuilders
    uuid rebuild( uint64_t ab, uint64_t cd );
    uuid rebuild( const std::string &uustr );
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4127)
#endif

namespace std {
    template<>
    struct hash< sole::uuid > {
    public:
        // hash functor: hash uuid to size_t value by pseudorandomizing transform
        size_t operator()( const sole::uuid &uuid ) const {
            if( sizeof(size_t) > 4 ) {
                return size_t( uuid.ab ^ uuid.cd );
            } else {
                uint64_t hash64 = uuid.ab ^ uuid.cd;
                return size_t( uint32_t( hash64 >> 32 ) ^ uint32_t( hash64 ) );
            }
        }
    };
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

// implementation

#include <memory.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include <cstring>
#include <ctime>

#include <iomanip>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#if defined(_WIN32)
#   include <winsock2.h>
#   include <process.h>
#   include <iphlpapi.h>
#   ifdef _MSC_VER
#   pragma comment(lib,"iphlpapi.lib")
#   endif
#   define $windows $yes
#elif defined(__FreeBSD__) || defined(__NetBSD__) || \
        defined(__OpenBSD__) || defined(__MINT__) || defined(__bsdi__)
#   include <ifaddrs.h>
#   include <net/if_dl.h>
#   include <sys/socket.h>
#   include <sys/time.h>
#   include <sys/types.h>
#   include <unistd.h>
#   define $bsd $yes
#elif (defined(__APPLE__) && defined(__MACH__))
#   include <ifaddrs.h>
#   include <net/if_dl.h>
#   include <sys/socket.h>
#   include <sys/time.h>
#   include <sys/types.h>
#   include <unistd.h>
#   pragma clang diagnostic push
#   pragma clang diagnostic ignored "-Wdollar-in-identifier-extension"
#   define $osx $yes
#elif defined(__linux__)
#   include <arpa/inet.h>
#   include <ifaddrs.h>
#   include <net/if.h>
#   include <netinet/in.h>
#   include <sys/ioctl.h>
#   include <sys/socket.h>
#   include <sys/time.h>
#   include <unistd.h>
#   define $linux $yes
#else //elif defined(__unix__)
#	if defined(__EMSCRIPTEN__)
#		define $emscripten $yes
#	endif
#   if defined(__VMS)
#      include <ioctl.h>
#      include <inet.h>
#   else
#      include <sys/ioctl.h>
#      include <arpa/inet.h>
#   endif
#   if defined(sun) || defined(__sun)
#      include <sys/sockio.h>
#   endif
#   include <net/if.h>
#   include <net/if_arp.h>
#   include <netdb.h>
#   include <netinet/in.h>
#   include <sys/socket.h>
#   include <sys/time.h>
#   include <sys/types.h>
#   include <unistd.h>
#   define $unix $yes
#endif

#ifdef _MSC_VER
#   define $msvc  $yes
#endif

#ifdef _MSC_VER
#   define $thread __declspec(thread)
#elif defined __clang__
#   define $thread thread_local
#else
#   define $thread __thread
#endif

#if defined(__GNUC__) && (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 < 50100)
    namespace std
    {
        static inline std::string put_time( const std::tm* tmb, const char* fmt ) {
            std::string s( 128, '\0' );
            while( !strftime( &s[0], s.size(), fmt, tmb ) )
                s.resize( s.size() + 128 );
            return s;
        }
    }
#endif

////////////////////////////////////////////////////////////////////////////////////

#ifdef  $windows
#define $welse   $no
#else
#define $windows $no
#define $welse   $yes
#endif

#ifdef  $bsd
#define $belse   $no
#else
#define $bsd     $no
#define $belse   $yes
#endif

#ifdef  $linux
#define $lelse   $no
#else
#define $linux   $no
#define $lelse   $yes
#endif

#ifdef  $unix
#define $uelse   $no
#else
#define $unix    $no
#define $uelse   $yes
#endif

#ifdef  $osx
#define $oelse   $no
#else
#define $osx     $no
#define $oelse   $yes
#endif

#ifdef  $msvc
#define $melse   $no
#else
#define $msvc    $no
#define $melse   $yes
#endif

#ifdef $emscripten
#define $emselse	$no
#else
#define $emselse	$yes
#endif

#define $yes(...) __VA_ARGS__
#define $no(...)

inline bool sole::uuid::operator==( const sole::uuid &other ) const {
    return ab == other.ab && cd == other.cd;
}
inline bool sole::uuid::operator!=( const sole::uuid &other ) const {
    return !operator==(other);
}
inline bool sole::uuid::operator<( const sole::uuid &other ) const {
    if( ab < other.ab ) return true;
    if( ab > other.ab ) return false;
    if( cd < other.cd ) return true;
    return false;
}

namespace sole {

    inline std::string printftime( uint64_t timestamp_secs = 0, const std::string &locale = std::string() ) {
        std::string timef;
        try {
            // Taken from parameter
            //std::string locale; // = "es-ES", "Chinese_China.936", "en_US.UTF8", etc...
            std::time_t t = timestamp_secs;
            std::tm tm;
            $msvc(
                localtime_s( &tm, &t );
            )
            $melse(
                $windows(tm = *localtime( &t ); )
                $welse( localtime_r(&t, &tm); )
            )

            std::stringstream ss;
            $melse(
            std::locale lc( locale.c_str() );
            ss.imbue( lc );
            )
            ss << std::put_time( &tm, "\"%c\"" );

            timef = ss.str();
        }
        catch(...) {
            timef = "\"\"";
        }
        return timef;
    }

    inline std::string uuid::pretty() const {
        std::stringstream ss;

        uint64_t a = (ab >> 32);
        uint64_t b = (ab & 0xFFFFFFFF);
        uint64_t c = (cd >> 32);
        uint64_t d = (cd & 0xFFFFFFFF);

        int version = (b & 0xF000) >> 12;
        uint64_t timestamp = ((b & 0x0FFF) << 48 ) | (( b >> 16 ) << 32) | a; // in 100ns units

        ss << "version=" << (version) << ',';

        if( version == 1 )
            timestamp = timestamp - 0x01b21dd213814000ULL; // decrement Gregorian calendar

        ss << std::hex << std::nouppercase << std::setfill('0');
        version <= 1 && ss << "timestamp=" << printftime(timestamp/10000000) << ',';
        version <= 1 && ss << "mac=" << std::setw(4) << (c & 0xFFFF) << std::setw(8) << d << ',';
        version == 4 && ss << "randbits=" << std::setw(8) << (ab & 0xFFFFFFFFFFFF0FFFULL) << std::setw(8) << (cd & 0x3FFFFFFFFFFFFFFFULL) << ',';

        ss << std::dec;
        version == 0 && ss << "pid=" << std::setw(4) << (c >> 16 ) << ',';
        version == 1 && ss << "clock_seq=" << std::setw(4) << ((c >> 16) & 0x3FFF) << ',';

        return ss.str();
    }

    inline std::string uuid::str() const {
        char uustr[] = "00000000-0000-0000-0000-000000000000";
        constexpr char encode[] = "0123456789abcdef";

        size_t bit = 15;
        for( size_t i = 0; i < 18; i++ ) {
            if( i == 8 || i == 13 ) {
                continue;
            }
            uustr[i] = encode[ab>>4*bit&0x0f];
            bit--;
        }

        bit = 15;
        for( size_t i = 18; i < 36; i++ ) {
            if( i == 18 || i == 23 ) {
                continue;
            }
            uustr[i] = encode[cd>>4*bit&0x0f];
            bit--;
        }

        return std::string(uustr);
    }

    inline std::string uuid::base62() const {
        int base62len = 10 + 26 + 26;
        const char base62[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        char res[24], *end = &res[24]; *(--end) = '\0';
        uint64_t rem, AB = ab, CD = cd;
        do {
            rem = CD % base62len;
            *--end = base62[int(rem)];
            CD /= base62len;
        } while (CD > 0);
        *--end = '-';
        do {
            rem = AB % base62len;
            *--end = base62[int(rem)];
            AB /= base62len;
        } while (AB > 0);
        return end;
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // multiplatform clock_gettime()

    $windows(
        struct timespec {
            uint64_t tv_sec;
            uint64_t tv_nsec;
        };
        struct timezone {
            int  tz_minuteswest; /* minutes W of Greenwich */
            int  tz_dsttime;     /* type of dst correction */
        };
        inline int gettimeofday( struct timeval *tv, struct timezone *tz ) {
            FILETIME ft;
            uint64_t tmpres = 0;

            if( NULL != tv ) {
                GetSystemTimeAsFileTime(&ft);

                // The GetSystemTimeAsFileTime returns the number of 100 nanosecond
                // intervals since Jan 1, 1601 in a structure. Copy the high bits to
                // the 64 bit tmpres, shift it left by 32 then or in the low 32 bits.
                tmpres |= ft.dwHighDateTime;
                tmpres <<= 32;
                tmpres |= ft.dwLowDateTime;

                // Convert to microseconds by dividing by 10
                tmpres /= 10;

                // The Unix epoch starts on Jan 1 1970.  Need to subtract the difference
                // in seconds from Jan 1 1601.
                tmpres -= 11644473600000000ULL;

                // Finally change microseconds to seconds and place in the seconds value.
                // The modulus picks up the microseconds.
                tv->tv_sec = static_cast<long>(tmpres / 1000000UL);
                tv->tv_usec = (tmpres % 1000000UL);
            }

            if( NULL != tz ) {
                static bool once = true;
                if( once ) {
                    once = false;
                    _tzset();
                }

                long timezoneSecs = 0;
                int daylight = 0;

                $msvc(
                    _get_timezone(&timezoneSecs);
                    _get_daylight(&daylight);
                )
                $melse(
                    timezoneSecs = _timezone;
                    daylight = _daylight;
                )

                tz->tz_minuteswest = timezoneSecs / 60;
                tz->tz_dsttime = daylight;
            }

            return 0;
        }
    )
    $lelse( $belse( $emselse ( // if not linux, if not bsd, if not emscripten... valid for apple/win32
        inline int clock_gettime( int /*clk_id*/, struct timespec* t ) {
            struct timeval now;
            int rv = gettimeofday(&now, NULL);
            if( rv ) return rv;
            t->tv_sec  = now.tv_sec;
            t->tv_nsec = now.tv_usec * 1000;
            return 0;
        }
    )))

    //////////////////////////////////////////////////////////////////////////////////////
    // Timestamp and MAC interfaces

    // Returns number of 100ns intervals
    inline uint64_t get_time( uint64_t offset ) {
        struct timespec tp;
        clock_gettime(0 /*CLOCK_REALTIME*/, &tp);

        // Convert to 100-nanosecond intervals
        uint64_t uuid_time;
        uuid_time = tp.tv_sec * 10000000;
        uuid_time = uuid_time + (tp.tv_nsec / 100);

        // If the clock looks like it went backwards, or is the same, increment it.
        static uint64_t last_uuid_time = 0;
        if( last_uuid_time >= uuid_time )
            uuid_time = ++last_uuid_time;
        else
            last_uuid_time = uuid_time;

        uuid_time = uuid_time + offset;

        return uuid_time;
    }

    // Looks for first MAC address of any network device, any size.
    inline bool get_any_mac( std::vector<unsigned char> &_node ) {
    $windows({
        PIP_ADAPTER_INFO pAdapterInfo;
        PIP_ADAPTER_INFO pAdapter = 0;
        ULONG len    = sizeof(IP_ADAPTER_INFO);
        pAdapterInfo = reinterpret_cast<IP_ADAPTER_INFO*>(new char[len]);

        // Make an initial call to GetAdaptersInfo to get
        // the necessary size into len
        DWORD rc = GetAdaptersInfo(pAdapterInfo, &len);
        if (rc == ERROR_BUFFER_OVERFLOW)
        {
            delete [] reinterpret_cast<char*>(pAdapterInfo);
            pAdapterInfo = reinterpret_cast<IP_ADAPTER_INFO*>(new char[len]);
        }
        else if (rc != ERROR_SUCCESS)
        {
            return $no("cannot get network adapter list") false;
        }

        bool found = false, gotten = false;
        if (GetAdaptersInfo(pAdapterInfo, &len) == NO_ERROR)
        {
            gotten = true;

            pAdapter = pAdapterInfo;
            while (pAdapter && !found)
            {
                if (pAdapter->Type == MIB_IF_TYPE_ETHERNET && pAdapter->AddressLength > 0 )
                {
                    _node.resize( pAdapter->AddressLength );
                    std::memcpy(_node.data(), pAdapter->Address, _node.size() );
                    found = true;
                }
                pAdapter = pAdapter->Next;
            }
        }

        delete [] reinterpret_cast<char*>(pAdapterInfo);

        if( !gotten )
            return $no("cannot get network adapter list") false;

        if (!found)
            return $no("no Ethernet adapter found") false;

        return true;
    })

    $bsd({
        struct ifaddrs* ifaphead;
        int rc = getifaddrs(&ifaphead);
        if (rc) return $no("cannot get network adapter list") false;

        bool foundAdapter = false;
        for (struct ifaddrs* ifap = ifaphead; ifap; ifap = ifap->ifa_next)
        {
            if (ifap->ifa_addr && ifap->ifa_addr->sa_family == AF_LINK)
            {
                struct sockaddr_dl* sdl = reinterpret_cast<struct sockaddr_dl*>(ifap->ifa_addr);
                caddr_t ap = (caddr_t) (sdl->sdl_data + sdl->sdl_nlen);
                int alen = sdl->sdl_alen;
                if (ap && alen > 0)
                {
                    _node.resize( alen );
                    std::memcpy(_node.data(), ap, _node.size() );
                    foundAdapter = true;
                    break;
                }
            }
        }
        freeifaddrs(ifaphead);
        if (!foundAdapter) return $no("cannot determine MAC address (no suitable network adapter found)") false;
        return true;
    })

    $osx({
        struct ifaddrs* ifaphead;
        int rc = getifaddrs(&ifaphead);
        if (rc) return $no("cannot get network adapter list") false;

        bool foundAdapter = false;
        for (struct ifaddrs* ifap = ifaphead; ifap; ifap = ifap->ifa_next)
        {
            if (ifap->ifa_addr && ifap->ifa_addr->sa_family == AF_LINK)
            {
                struct sockaddr_dl* sdl = reinterpret_cast<struct sockaddr_dl*>(ifap->ifa_addr);
                caddr_t ap = (caddr_t) (sdl->sdl_data + sdl->sdl_nlen);
                int alen = sdl->sdl_alen;
                if (ap && alen > 0)
                {
                    _node.resize( alen );
                    std::memcpy(_node.data(), ap, _node.size() );
                    foundAdapter = true;
                    break;
                }
            }
        }
        freeifaddrs(ifaphead);
        if (!foundAdapter) return $no("cannot determine MAC address (no suitable network adapter found)") false;
        return true;
    })

    $linux({
        struct ifaddrs* ifaphead;
        if (getifaddrs(&ifaphead) == -1) return $no("cannot get network adapter list") false;

        bool foundAdapter = false;
        for (struct ifaddrs* ifap = ifaphead; ifap; ifap = ifap->ifa_next)
        {
            struct ifreq ifr;
            int s = socket(PF_INET, SOCK_DGRAM, 0);
            if (s == -1) continue;

            if (std::strcmp("lo", ifap->ifa_name) == 0) { close(s); continue;}  // loopback address is zero

            std::strcpy(ifr.ifr_name, ifap->ifa_name);
            int rc = ioctl(s, SIOCGIFHWADDR, &ifr);
            close(s);
            if (rc < 0) continue;
            struct sockaddr* sa = reinterpret_cast<struct sockaddr*>(&ifr.ifr_addr);
            _node.resize( sizeof(sa->sa_data) );
            std::memcpy(_node.data(), sa->sa_data, _node.size() );
            foundAdapter = true;
            break;
        }
        freeifaddrs(ifaphead);
        if (!foundAdapter) return $no("cannot determine MAC address (no suitable network adapter found)") false;
        return true;

    })

    $unix({
        char name[HOST_NAME_MAX];
        if (gethostname(name, sizeof(name)))
            return $no("cannot get host name") false;

        struct hostent* pHost = gethostbyname(name);
        if (!pHost) return $no("cannot get host IP address") false;

        int s = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (s == -1) return $no("cannot open socket") false;

        struct arpreq ar;
        std::memset(&ar, 0, sizeof(ar));
        struct sockaddr_in* pAddr = reinterpret_cast<struct sockaddr_in*>(&ar.arp_pa);
        pAddr->sin_family = AF_INET;
        std::memcpy(&pAddr->sin_addr, *pHost->h_addr_list, sizeof(struct in_addr));
        int rc = ioctl(s, SIOCGARP, &ar);
        close(s);
        if (rc < 0) return $no("cannot get MAC address") false;
        _node.resize( sizeof(ar.arp_ha.sa_data) );
        std::memcpy(_node.data(), ar.arp_ha.sa_data, _node.size());
        return true;
    })
    }

    // Looks for first MAC address of any network device, size truncated to 48bits.
    inline uint64_t get_any_mac48() {
        std::vector<unsigned char> node;
        if( get_any_mac(node) ) {
            std::stringstream ss;
            ss << std::hex << std::setfill('0');
            node.resize(6);
            for( unsigned i = 0; i < 6; ++i )
                ss << std::setw(2) << int(node[i]);
            uint64_t t;
            if( ss >> t )
                return t;
        }
        return 0;
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // UUID implementations

    inline uuid uuid4() {
        static $thread std::random_device rd;
        static $thread std::uniform_int_distribution<uint64_t> dist(0, (uint64_t)(~0));

        uuid my;

        my.ab = dist(rd);
        my.cd = dist(rd);

        my.ab = (my.ab & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
        my.cd = (my.cd & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;

        return my;
    }

    inline uuid uuid1() {
        // Number of 100-ns intervals since 00:00:00.00 15 October 1582; [ref] uuid.py
        uint64_t ns100_intervals = get_time( 0x01b21dd213814000ULL );
        uint16_t clock_seq = (uint16_t)( ns100_intervals & 0x3fff );  // 14-bits max
        uint64_t mac = get_any_mac48();                               // 48-bits max

        uint32_t time_low = ns100_intervals & 0xffffffff;
        uint16_t time_mid = (ns100_intervals >> 32) & 0xffff;
        uint16_t time_hi_version = (ns100_intervals >> 48) & 0xfff;
        uint8_t clock_seq_low = clock_seq & 0xff;
        uint8_t clock_seq_hi_variant = (clock_seq >> 8) & 0x3f;

        uuid u;
        uint64_t &upper_ = u.ab;
        uint64_t &lower_ = u.cd;

        // Build the high 32 bytes
        upper_  = (uint64_t) time_low << 32;
        upper_ |= (uint64_t) time_mid << 16;
        upper_ |= (uint64_t) time_hi_version;

        // Build the low 32 bytes, using the clock sequence number
        lower_  = (uint64_t) ((clock_seq_hi_variant << 8) | clock_seq_low) << 48;
        lower_ |= mac;

        // Set the variant to RFC 4122.
        lower_ &= ~((uint64_t)0xc000 << 48);
        lower_ |=   (uint64_t)0x8000 << 48;

        // Set the version number.
        enum { version = 1 };
        upper_ &= ~0xf000;
        upper_ |= version << 12;

        return u;
    }

    inline uuid uuid0() {
        // Number of 100-ns intervals since Unix epoch time
        uint64_t ns100_intervals = get_time( 0 );
        uint64_t pid = $windows( _getpid() ) $welse( getpid() );
        uint16_t pid16 = (uint16_t)( pid & 0xffff ); // 16-bits max
        uint64_t mac = get_any_mac48();              // 48-bits max

        uint32_t time_low = ns100_intervals & 0xffffffff;
        uint16_t time_mid = (ns100_intervals >> 32) & 0xffff;
        uint16_t time_hi_version = (ns100_intervals >> 48) & 0xfff;
        uint8_t pid_low = pid16 & 0xff;
        uint8_t pid_hi = (pid16 >> 8) & 0xff;

        uuid u;
        uint64_t &upper_ = u.ab;
        uint64_t &lower_ = u.cd;

        // Build the high 32 bytes.
        upper_  = (uint64_t) time_low << 32;
        upper_ |= (uint64_t) time_mid << 16;
        upper_ |= (uint64_t) time_hi_version;

        // Build the low 32 bytes, using the mac and pid number.
        lower_  = (uint64_t) ((pid_hi << 8) | pid_low) << 48;
        lower_ |= mac;

        // Set the version number.
        enum { version = 0 };
        upper_ &= ~0xf000;
        upper_ |= version << 12;

        return u;
    }

    inline uuid rebuild( uint64_t ab, uint64_t cd ) {
        uuid u;
        u.ab = ab; u.cd = cd;
        return u;
    }

    inline uuid rebuild( const std::string &uustr ) {
        uuid u = { 0, 0 };
        auto idx = uustr.find_first_of("-");
        if( idx != std::string::npos ) {
            // single separator, base62 notation
            if( uustr.find_first_of("-",idx+1) == std::string::npos ) {
                auto rebase62 = [&]( const char *input, size_t limit ) -> uint64_t {
                    int base62len = 10 + 26 + 26;
                    auto strpos = []( char ch ) -> size_t {
                        if( ch >= 'a' ) return ch - 'a' + 10 + 26;
                        if( ch >= 'A' ) return ch - 'A' + 10;
                        return ch - '0';
                    };
                    uint64_t res = strpos( input[0] );
                    for( size_t i = 1; i < limit; ++i )
                        res = base62len * res + strpos( input[i] );
                    return res;
                };
                u.ab = rebase62( &uustr[0], idx );
                u.cd = rebase62( &uustr[idx+1], uustr.size() - (idx+1) );
            }
            // else classic hex notation
            else {
                if (uustr[8] == '-' || uustr[13] == '-' || uustr[18] == '-' || uustr[23] == '-') {
                    auto decode = []( char ch ) -> size_t {
                        if( 'f' >= ch && ch >= 'a' ) return ch - 'a' + 10;
                        if( 'F' >= ch && ch >= 'A' ) return ch - 'A' + 10;
                        if( '9' >= ch && ch >= '0' ) return ch - '0';
                        return 0;
                    };
                    for( size_t i = 0; i < 18; i++ ) {
                        if( i == 8 || i == 13 ) {
                            continue;
                        }
                        u.ab = u.ab<<4 | decode(uustr[i]);
                    }
                    for( size_t i = 19; i < 36; i++ ) {
                        if( i == 23 ) {
                            continue;
                        }
                        u.cd = u.cd<<4 | decode(uustr[i]);
                    }
                }
            }
        }
        return u;
    }

} // ::sole

#undef $bsd
#undef $belse
#undef $linux
#undef $lelse
#undef $osx
#undef $oelse
#undef $unix
#undef $uelse
#undef $windows
#undef $welse
#undef $yes
#undef $no

// Pop disabled warnings
#if (defined(__APPLE__) && defined(__MACH__))
#pragma clang diagnostic pop
#endif

#ifdef SOLE_BUILD_DEMO
// g++ users: `g++ demo.cc -std=c++11 -lrt -o sample`
// visual studio: `cl.exe demo.cc sole.cpp`

#include <iostream>

int main() {
    sole::uuid u0 = sole::uuid0(), u1 = sole::uuid1(), u4 = sole::uuid4();

    std::cout << "uuid v0 string : " << u0 << std::endl;
    std::cout << "uuid v0 base62 : " << u0.base62() << std::endl;
    std::cout << "uuid v0 pretty : " << u0.pretty() << std::endl << std::endl;

    std::cout << "uuid v1 string : " << u1 << std::endl;
    std::cout << "uuid v1 base62 : " << u1.base62() << std::endl;
    std::cout << "uuid v1 pretty : " << u1.pretty() << std::endl << std::endl;

    std::cout << "uuid v4 string : " << u4 << std::endl;
    std::cout << "uuid v4 base62 : " << u4.base62() << std::endl;
    std::cout << "uuid v4 pretty : " << u4.pretty() << std::endl << std::endl;

    u1 = sole::rebuild("F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6");
    u4 = sole::rebuild("GITheR4tLlg-BagIW20DGja");

    std::cout << "uuid v1 rebuilt : " << u1 << " -> " << u1.pretty() << std::endl;
    std::cout << "uuid v4 rebuilt : " << u4 << " -> " << u4.pretty() << std::endl;
}

#endif

#ifdef SOLE_BUILD_TESTS
// g++ users: `g++ sole.cxx -std=c++11 -lrt -o tests`
// visual studio: `cl.exe sole.cxx`

#include <cassert>
#include <set>
#include <ratio>
#include <chrono>
#include <iostream>

using namespace sole;

namespace run
{
    auto epoch = [](){
        return std::chrono::system_clock::to_time_t( std::chrono::system_clock::now() );
    };

    template<typename FN>
    void benchmark( const FN &fn, const std::string &name ) {
        std::cout << "Benchmarking " << name << "... " << std::flush;

        auto then = epoch();

        while( epoch() == then );
        then = epoch();

        unsigned c = 0;
        while( epoch() == then ) c = ( fn(), ++c );

        std::cout << (c) << " uuids/sec" << std::endl;
    }

    template<typename FN>
    void tests( const FN &fn ) {
        unsigned numtests = ~0;
        std::cout << "Testing for " << numtests << " collisions... " << std::endl;

        auto then = epoch();

        std::set<uuid> all;
        for( unsigned i = 0; i < numtests; ++i ) {
            auto now = epoch();
            if( now != then ) {
                then = now;
                double pct6digs = ( int( ( double(i) / (unsigned)(~0) ) * 1e4 ) / double(1e4) );
                std::cout << '\r' << i << " uuids generated, no collision (" << pct6digs << "%)" << std::flush;
            }
            sole::uuid my_uuid = fn();
            assert( all.find(my_uuid) == all.end() && "error: UUIDs just collided! is std::random_device a real random generator?" );
            all.insert( my_uuid );
        }
    }

    template<typename FN>
    void verify( const FN &fn ) {
        std::cout << "Verifying serialization of 1 million UUIDs... " << std::flush;

        for( unsigned i = 0; i < 1000000; ++i ) {
            sole::uuid uuid = fn();
            sole::uuid rebuilt1 = sole::rebuild( uuid.str() );
            sole::uuid rebuilt2 = sole::rebuild( uuid.base62() );
            assert( rebuilt1 == uuid && "error: rebuild() or .str() failed" );
            assert( rebuilt2 == uuid && "error: rebuild() or .base62() failed" );
        }

        std::cout << "ok" << std::endl;
    }
}

int main() {
    assert( sizeof(sole::uuid      )  * 8 == 128 );
    assert( sizeof(sole::uuid0().ab)  * 8 ==  64 );
    assert( sizeof(sole::uuid0().cd)  * 8 ==  64 );

    run::benchmark(uuid0, "v0");
    run::benchmark(uuid1, "v1");
    run::benchmark(uuid4, "v4");

    auto uustr = uuid4().str();
    run::benchmark([=]() { sole::rebuild( uustr ); }, "rebuild");

    auto uuid = uuid4();
    run::benchmark([=]() { uuid.str(); }, "str");

    run::verify(uuid4);             // use fastest implementation

//  run::tests(uuid0);              // not applicable
//  run::tests(uuid1);              // not applicable
    run::tests(uuid4);
}

#endif

