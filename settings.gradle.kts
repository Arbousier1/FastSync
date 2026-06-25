rootProject.name = "FastSync"

// =============================================================================
// Sparrow 库（Gradle Composite Builds）
// =============================================================================
//
// sparrow-nbt / sparrow-yaml / sparrow-redis-message-broker 由 Xiao-MoMi
// 维护，仅发布到私有仓库 https://repo.momirealms.net/releases（需凭据）。
// CI 环境和普通开发者无法解析这些外部坐标。
//
// 解决方案：将 Sparrow 仓库作为 git 子模块引入，并使用 Gradle Composite
// Builds（includeBuild）+ 显式 dependencySubstitution，
// 让 Gradle 在解析 net.momirealms:sparrow-* 时自动替换为本地项目依赖。
//
// 子模块路径：sparrow/sparrow-*
// 配置文件：.gitmodules
// =============================================================================

includeBuild("sparrow/sparrow-nbt") {
    dependencySubstitution {
        substitute(module("net.momirealms:sparrow-nbt"))
            .using(project(":core"))
    }
}

includeBuild("sparrow/sparrow-yaml") {
    dependencySubstitution {
        substitute(module("net.momirealms:sparrow-yaml"))
            .using(project(":common"))
    }
}

includeBuild("sparrow/sparrow-redis-message-broker") {
    dependencySubstitution {
        substitute(module("net.momirealms:sparrow-redis-message-broker"))
            .using(project(":core"))
    }
}