val repoUrl: String by project
var beamVersion = "2.9.0"

plugins {
    id("net.ltgt.apt") version "0.20"
    id("com.google.protobuf") version "0.8.8"
    `java-library`
    `maven-publish`
    idea
}

dependencies {
    compile(project(":convert"))
    compile("org.apache.beam:beam-sdks-java-core:$beamVersion")
    compile("org.apache.beam:beam-runners-direct-java:$beamVersion")
    compile("org.apache.beam:beam-runners-google-cloud-dataflow-java:$beamVersion")
    compile("org.apache.beam:beam-sdks-java-extensions-protobuf:$beamVersion")
    compile("org.apache.beam:beam-sdks-java-extensions-google-cloud-platform-core:$beamVersion")
    compile("org.apache.beam:beam-sdks-java-io-google-cloud-platform:$beamVersion")
    compile("org.apache.beam:beam-sdks-java-io-common:$beamVersion")
    testImplementation("org.apache.beam:beam-sdks-java-extensions-sql:$beamVersion")
    testImplementation("junit:junit:4.12")
    testImplementation("org.hamcrest:hamcrest-all:1.3")
}

protobuf {

}

publishing {
    repositories {
        maven {
            // change to point to your repo, e.g. http://my.org/repo
            url = uri(repoUrl)
        }
    }
    publications {
        register("mavenJava", MavenPublication::class) {
            from(components["java"])
        }
    }
}