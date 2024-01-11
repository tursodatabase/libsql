import os
from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMakeDeps, CMakeToolchain, CMake
from conan.tools.files import get, copy

required_conan_version = ">=2.0"


class sqlite3mc(ConanFile):
    name = "sqlite3mc"
    version = "1.8.0"
    package_type = "library"

    license = "MIT"
    author = "utelle(GitHub)"
    homepage = "https://github.com/utelle/SQLite3MultipleCiphers"
    url = "https://github.com/conan-io/conan-center-index"
    description = "The project SQLite3 Multiple Ciphers implements an encryption extension for SQLite with support for multiple ciphers."
    topics = ("sqlite", "sqlite3", "sqlite3-encryption", "database-encryption", "sqlite3-extension")

    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "require_zlib": [True, False],
        "static_runtime_link": [True, False],
        "build_shell": [True, False],
        "with_icu": [True, False],
        "enable_debug": [True, False],
        "soundex": [True, False],
        "enable_column_metadata": [True, False],
        "secure_delete": [True, False],
        "enable_fts3": [True, False],
        "enable_fts3_paranthesis": [True, False],
        "enable_fts4": [True, False],
        "enable_fts5": [True, False],
        "enable_carray": [True, False],
        "enable_csv": [True, False],
        "enable_extfunc": [True, False],
        "enable_geopoly": [True, False],
        "enable_json1": [True, False],
        "enable_rtree": [True, False],
        "enable_uuid": [True, False],
        "use_uri": [True, False],
        "user_authentication": [True, False],
        "enable_preupdate_hook": [True, False],
        "enable_session": [True, False],
        "shell_is_utf8": [True, False],
        "enable_fileio": [True, False],
        "enable_regexp": [True, False],
        "enable_series": [True, False],
        "enable_sha3": [True, False],
        "enable_explain_comments": [True, False],
        "enable_dbpage_vtab": [True, False],
        "enable_dbstat_vtab": [True, False],
        "enable_stmtvtab": [True, False],
        "enable_unknown_sql_function": [True, False],
        "use_miniz": [True, False],
        "enable_compress": [True, False],
        "enable_sqlar": [True, False],
        "enable_zipfile": [True, False],
        "use_sqleet_legacy": [True, False],
        "use_sqlcipher_legacy": [True, False],
        "secure_memory": [True, False],
        "use_random_fill_memory": [True, False],
        "omit_aes_hardware_support": [True, False]
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "require_zlib": False,
        "static_runtime_link": False,
        "build_shell": False,
        "with_icu": False,
        "enable_debug": False,
        "soundex": True,
        "enable_column_metadata": True,
        "secure_delete": True,
        "enable_fts3": True,
        "enable_fts3_paranthesis": True,
        "enable_fts4": True,
        "enable_fts5": True,
        "enable_carray": True,
        "enable_csv": True,
        "enable_extfunc": True,
        "enable_geopoly": True,
        "enable_json1": True,
        "enable_rtree": True,
        "enable_uuid": True,
        "use_uri": True,
        "user_authentication": True,
        "enable_preupdate_hook": False,
        "enable_session": False,
        "shell_is_utf8": True,
        "enable_fileio": True,
        "enable_regexp": True,
        "enable_series": True,
        "enable_sha3": True,
        "enable_explain_comments": True,
        "enable_dbpage_vtab": True,
        "enable_dbstat_vtab": True,
        "enable_stmtvtab": True,
        "enable_unknown_sql_function": True,
        "use_miniz": False,
        "enable_compress": False,
        "enable_sqlar": False,
        "enable_zipfile": False,
        "use_sqleet_legacy": False,
        "use_sqlcipher_legacy": False,
        "secure_memory": False,
        "use_random_fill_memory": False,
        "omit_aes_hardware_support": False
    }

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def source(self):
        get(self, **self.conan_data["sources"][self.version])

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        if self.options.require_zlib:
            self.requires("zlib/[>=1.2.9]")
        if self.options.with_icu:
            self.requires("icu/[>=67.1]")

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.variables["_SQLITE3MC_REQUIRE_ZLIB"] = self.options.require_zlib
        tc.variables["SQLITE3MC_STATIC_RUNTIME_LINK"] = self.options.static_runtime_link
        tc.variables["SQLITE3MC_STATIC"] = not self.options.shared
        tc.variables["SQLITE3MC_BUILD_SHELL"] = self.options.build_shell
        tc.variables["SQLITE3MC_WITH_ICU"] = self.options.with_icu

        tc.variables["SQLITE_ENABLE_DEBUG"] = self.options.enable_debug
        tc.variables["SQLITE_SOUNDEX"] = self.options.soundex
        tc.variables["SQLITE_ENABLE_COLUMN_METADATA"] = self.options.enable_column_metadata
        tc.variables["SQLITE_SECURE_DELETE"] = self.options.secure_delete
        tc.variables["SQLITE_ENABLE_FTS3"] = self.options.enable_fts3
        tc.variables["SQLITE_ENABLE_FTS3_PARENTHESIS"] = self.options.enable_fts3_paranthesis
        tc.variables["SQLITE_ENABLE_FTS4"] = self.options.enable_fts4
        tc.variables["SQLITE_ENABLE_FTS5"] = self.options.enable_fts5

        tc.variables["SQLITE_ENABLE_CARRAY"] = self.options.enable_carray
        tc.variables["SQLITE_ENABLE_CSV"] = self.options.enable_csv
        tc.variables["SQLITE_ENABLE_EXTFUNC"] = self.options.enable_extfunc
        tc.variables["SQLITE_ENABLE_GEOPOLY"] = self.options.enable_geopoly
        tc.variables["SQLITE_ENABLE_JSON1"] = self.options.enable_json1
        tc.variables["SQLITE_ENABLE_RTREE"] = self.options.enable_rtree
        tc.variables["SQLITE_ENABLE_UUID"] = self.options.enable_uuid
        tc.variables["SQLITE_USE_URI"] = self.options.use_uri
        tc.variables["SQLITE_USER_AUTHENTICATION"] = self.options.user_authentication
        tc.variables["SQLITE_ENABLE_PREUPDATE_HOOK"] = self.options.enable_preupdate_hook
        tc.variables["SQLITE_ENABLE_SESSION"] = self.options.enable_session
        tc.variables["SQLITE_SHELL_IS_UTF8"] = self.options.shell_is_utf8

        # Options for library only
        tc.variables["SQLITE_ENABLE_FILEIO"] = self.options.enable_fileio
        tc.variables["SQLITE_ENABLE_REGEXP"] = self.options.enable_regexp
        tc.variables["SQLITE_ENABLE_SERIES"] = self.options.enable_series
        tc.variables["SQLITE_ENABLE_SHA3"] = self.options.enable_sha3

        # Options for shell only (compatibility with official SQLite shell)
        tc.variables["SQLITE_ENABLE_EXPLAIN_COMMENTS"] = self.options.enable_explain_comments
        tc.variables["SQLITE_ENABLE_DBPAGE_VTAB"] = self.options.enable_dbpage_vtab
        tc.variables["SQLITE_ENABLE_DBSTAT_VTAB"] = self.options.enable_dbstat_vtab
        tc.variables["SQLITE_ENABLE_STMTVTAB"] = self.options.enable_stmtvtab
        tc.variables["SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION"] = self.options.enable_unknown_sql_function

        # Embedded Compression
        tc.variables["SQLITE3MC_USE_MINIZ"] = self.options.use_miniz

        # Compression/Options that require ZLIB
        tc.variables["SQLITE_ENABLE_COMPRESS"] = self.options.enable_compress
        tc.variables["SQLITE_ENABLE_SQLAR"] = self.options.enable_sqlar
        tc.variables["SQLITE_ENABLE_ZIPFILE"] = self.options.enable_zipfile

        # Legacy Encryption Extensions
        tc.variables["SQLITE3MC_USE_SQLEET_LEGACY"] = self.options.use_sqleet_legacy
        tc.variables["SQLITE3MC_USE_SQLCIPHER_LEGACY"] = self.options.use_sqlcipher_legacy

        # Additional memory security (filling freed memory allocations with zeros or random data)
        tc.variables["SQLITE3MC_SECURE_MEMORY"] = self.options.secure_memory
        tc.variables["SQLITE3MC_USE_RANDOM_FILL_MEMORY"] = self.options.use_random_fill_memory

        # Omit AES hardware support
        tc.variables["SQLITE3MC_OMIT_AES_HARDWARE_SUPPORT"] = self.options.omit_aes_hardware_support

        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()
        copy(self, "LICENSE*", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"), keep_path=False)

    def package_info(self):
        if self.options.shared:
            self.cpp_info.libs = ["sqlite3mc"]
        else:
            self.cpp_info.libs = ["sqlite3mc_static"]
        if self.settings.os in ("Linux", "Macos"):
            self.cpp_info.system_libs.append("pthread")
            self.cpp_info.system_libs.append("dl")
            self.cpp_info.system_libs.append("m")
            if self.settings.os == "Macos":
                self.cpp_info.frameworks.append("Security")
