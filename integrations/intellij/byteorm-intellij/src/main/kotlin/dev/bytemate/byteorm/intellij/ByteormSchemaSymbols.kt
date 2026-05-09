package dev.bytemate.byteorm.intellij

import com.intellij.openapi.project.DumbService
import com.intellij.openapi.project.Project
import com.intellij.openapi.vfs.VfsUtilCore
import com.intellij.openapi.vfs.VirtualFile
import com.intellij.psi.search.FileTypeIndex
import com.intellij.psi.search.GlobalSearchScope

data class ByteormSymbols(
    val models: List<String>,
    val enums: List<String>,
    val enumValues: List<String>,
    val fieldsByModel: Map<String, List<String>>,
)

object ByteormSchemaSymbols {
    fun collectFromText(text: String): ByteormSymbols {
        val parsed = ByteormSchemaParser.parse(text)
        return ByteormSymbols(
            models = parsed.models,
            enums = parsed.enums,
            enumValues = parsed.enumValuesByEnum.values.flatten().distinct(),
            fieldsByModel = parsed.fieldsByModel,
        )
    }

    fun collect(project: Project, currentFile: VirtualFile? = null): ByteormSymbols {
        val modelNames = linkedSetOf<String>()
        val enumNames = linkedSetOf<String>()
        val enumValues = linkedSetOf<String>()
        val fieldsByModel = linkedMapOf<String, LinkedHashSet<String>>()

        if (!DumbService.isDumb(project)) {
            val scope = GlobalSearchScope.projectScope(project)
            val files = FileTypeIndex.getFiles(ByteormFileType, scope)
            for (file in files) {
                readFile(file, modelNames, enumNames, enumValues, fieldsByModel)
            }
        } else if (currentFile != null) {
            readFile(currentFile, modelNames, enumNames, enumValues, fieldsByModel)
        }

        return ByteormSymbols(
            models = modelNames.toList(),
            enums = enumNames.toList(),
            enumValues = enumValues.toList(),
            fieldsByModel = fieldsByModel.mapValues { it.value.toList() },
        )
    }

    private fun readFile(
        file: VirtualFile,
        modelNames: MutableSet<String>,
        enumNames: MutableSet<String>,
        enumValues: MutableSet<String>,
        fieldsByModel: MutableMap<String, LinkedHashSet<String>>,
    ) {
        val text = runCatching { VfsUtilCore.loadText(file) }.getOrNull() ?: return
        ByteormSchemaParser.parse(text).let { parsed ->
            modelNames.addAll(parsed.models)
            enumNames.addAll(parsed.enums)
            parsed.enumValuesByEnum.values.forEach { enumValues.addAll(it) }
            for ((model, fields) in parsed.fieldsByModel) {
                fieldsByModel.getOrPut(model) { linkedSetOf() }.addAll(fields)
            }
        }
    }
}
