plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.plugin.rrcloud"
    compileSdk = 36

    defaultConfig {
        // Matches the app module's own `minSdk` (`gen/android/app/build.
        // gradle.kts`) — the real deployment floor ARCHITECTURE.md §5
        // designs against (WorkManager/Keystore/MediaStore generation
        // counter all assume at least this).
        minSdk = 24

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        consumerProguardFiles("consumer-rules.pro")
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
    kotlinOptions {
        jvmTarget = "1.8"
    }
}

dependencies {

    implementation("androidx.core:core-ktx:1.9.0")
    implementation("androidx.appcompat:appcompat:1.6.0")
    implementation("com.google.android.material:material:1.7.0")
    // ARCHITECTURE.md §5.3: `SyncCycleWorker`/`DcimScanWorker` scheduling
    // (§5.1/§5.2).
    implementation("androidx.work:work-runtime-ktx:2.9.1")
    // ARCHITECTURE.md §5.1/§5.3: the Keystore-backed `EncryptedSharedPreferences`
    // `AndroidCredentialStore` wraps.
    implementation("androidx.security:security-crypto:1.1.0-alpha06")
    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test.ext:junit:1.1.5")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.5.1")
    implementation(project(":tauri-android"))
}
