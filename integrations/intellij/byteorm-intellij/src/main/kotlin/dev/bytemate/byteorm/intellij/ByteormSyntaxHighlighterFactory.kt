package dev.bytemate.byteorm.intellij

import com.intellij.openapi.fileTypes.SyntaxHighlighter
import com.intellij.openapi.fileTypes.SyntaxHighlighterFactory
import com.intellij.openapi.project.Project
import com.intellij.openapi.vfs.VirtualFile

class ByteormSyntaxHighlighterFactory : SyntaxHighlighterFactory() {
    override fun getSyntaxHighlighter(project: Project?, virtualFile: VirtualFile?): SyntaxHighlighter {
        val symbols = if (project != null) ByteormSchemaSymbols.collect(project, virtualFile) else null
        return ByteormSyntaxHighlighter(symbols)
    }
}
