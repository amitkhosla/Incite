import os
import javalang
import hashlib
import logging

# Configure logging to see errors clearly
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class JavaExtractor:
    def __init__(self):
        pass

    def extract_from_directory(self, directory):
        """Walks through the directory and parses all .java files."""
        all_methods = []
        if not os.path.exists(directory):
            logging.error(f"Directory Error: {directory} does not exist.")
            return []

        for root, _, files in os.walk(directory):
            for file in files:
                # Skip system files and non-java files
                if file.endswith(".java") and "module-info.java" not in file:
                    file_path = os.path.join(root, file)
                    methods = self.parse_file(file_path, directory)
                    all_methods.extend(methods)
        return all_methods

    def parse_file(self, file_path, repo_root):
        """High-level coordination of file parsing with detailed error reporting."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            # Attempt to build the AST
            try:
                tree = javalang.parse.parse(code)
            except javalang.parser.JavaSyntaxError as se:
                logging.warning(f"Line {se.at}: Syntax Error in {file_path}. Javalang may not support this Java version's syntax.")
                return []
            except Exception as e:
                logging.warning(f"Failed to parse {file_path}: {e}")
                return []

            rel_path = os.path.relpath(file_path, repo_root)
            lines = code.splitlines()
            
            file_context = {
                "rel_path": rel_path,
                "package_name": tree.package.name if tree.package else "default",
                "imports": [imp.path for imp in tree.imports],
                "component": rel_path.split(os.sep)[0]
            }

            return self._extract_methods_from_tree(tree, lines, file_context)
            
        except Exception as e:
            logging.error(f"Unexpected error processing {file_path}: {e}")
            return []

    def _extract_methods_from_tree(self, tree, lines, ctx):
        """Iterates through the AST to find and process method declarations."""
        extracted_methods = []
        # Registry to prevent 'ON CONFLICT' crashes within the same file batch
        file_id_registry = set() 

        for path, node in tree.filter(javalang.tree.MethodDeclaration):
            # 1. Identity: Get Structural Path (Handles Nesting/Anonymous)
            class_path = self._get_nested_class_path(path)
            params_sig = self._get_params_signature(node)
            
            # 2. Stable ID Generation
            base_sig = f"{ctx['rel_path']}#{class_path}#{node.name}({params_sig})"
            
            # Resolve collisions (e.g., two lambdas/anon classes on same line)
            final_sig = base_sig
            counter = 1
            while final_sig in file_id_registry:
                final_sig = f"{base_sig}#sub_{counter}"
                counter += 1
            file_id_registry.add(final_sig)
            
            method_id = hashlib.md5(final_sig.encode()).hexdigest()

            # 3. Code & Metrics Extraction
            method_code, line_count = self._get_method_body(lines, node.position.line - 1)
            content_hash = hashlib.md5(method_code.encode()).hexdigest()

            # 4. Dependency Analysis
            internal, external = self._analyze_dependencies(ctx['package_name'], ctx['imports'])

            extracted_methods.append({
                "id": method_id,
                "content_hash": content_hash,
                "source": ctx['rel_path'],
                "component": ctx['component'],
                "package": ctx['package_name'],
                "class_name": class_path,
                "method_name": node.name,
                "line_count": line_count,
                "visibility": self._get_visibility(node),
                "return_type": self._get_return_type(node),
                "method_code": method_code,
                "javadoc_raw": node.documentation or "",
                "internal_deps": ", ".join(internal[:10]),
                "external_libs": ", ".join(external[:10])
            })
        return extracted_methods

    # --- Helper Methods ---

    def _get_nested_class_path(self, path):
        """Builds a path like Class$InnerClass or Class$AnonL42."""
        parts = []
        for node in path:
            if isinstance(node, javalang.tree.ClassDeclaration):
                parts.append(node.name)
            elif isinstance(node, javalang.tree.ClassCreator):
                # javalang uses ClassCreator for 'new Type() { ... }'
                if node.body:
                    line = node.position.line if node.position else "unk"
                    parts.append(f"AnonL{line}")
        return "$".join(parts) if parts else "Unknown"

    def _get_params_signature(self, node):
        """Constructs a string of parameter types, including Varargs (...) support."""
        param_types = []
        for p in node.parameters:
            t_name = "unknown"
            if hasattr(p.type, 'name'):
                t_name = p.type.name
            
            # Check for Varargs (e.g., Supplier<K>... suppliers)
            if getattr(p, 'varargs', False):
                t_name += "..."
            param_types.append(t_name)
        return ",".join(param_types)

    def _get_return_type(self, node):
        if not node.return_type:
            return "void"
        return getattr(node.return_type, 'name', 'unknown')

    def _get_visibility(self, node):
        modifiers = ['public', 'private', 'protected']
        return next((m for m in node.modifiers if m in modifiers), "package-private")

    def _analyze_dependencies(self, package_name, all_imports):
        parts = package_name.split('.')
        prefix = ".".join(parts[:2]) if len(parts) > 1 else parts[0]
        internal = [i for i in all_imports if i.startswith(prefix)]
        external = [i for i in all_imports if not i.startswith(prefix)]
        return internal, external

    def _get_method_body(self, lines, start_line):
        """Extracts the method body by balancing braces."""
        body, brace_count, started = [], 0, False
        for line in lines[start_line:]:
            body.append(line)
            brace_count += line.count('{') - line.count('}')
            if '{' in line: started = True
            if started and brace_count <= 0: break
        
        return "\n".join(body), len(body)
