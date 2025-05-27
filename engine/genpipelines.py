import itertools

#&runPipeline<BasicScanner, 2, std::index_sequence<0, 1>, std::index_sequence<0, 0, 1>>;
def comma(seq):
    return ','.join(map(str, seq))

def jj(seq):
    return ''.join(map(str, seq))

def subsets_with_replacement(p, r):
    for i in range(0, r + 1):
        for v in itertools.combinations_with_replacement(p, i):
            yield v

targets = [
    "engine::HashtableBuild",
    "engine::TableTarget"
]
scans = [
    "engine::TableScan"
]

funcs = []
maxn = 3
maxattrs = 6

for i in range(0, maxn):
    for keys in itertools.product(range(maxn), repeat = i):
        print(i, keys)
        isValid = True
        for ind, v in enumerate(keys):
            if v > ind:
                isValid = False
                break
        if not isValid:
            continue

        for target in ["engine::TableTarget"]:
            for scan in scans:
                attrs = []
                restAttrs = []
                funcs.append((f'{target},{scan},{i},({jj(keys)}),({jj(attrs)})',f'PipelineFunctions::runPipeline<{target}, {scan}, {i}, std::index_sequence<{comma(keys)}>, std::index_sequence<{comma(attrs)}>>'))
        # Let the first attribute come from any table as it will be interpreted as the hash key by hash builds
        for firstAttr in range(i + 1):
            for restAttrs in subsets_with_replacement(range(i + 1), maxattrs):
                for target in targets:
                    for scan in scans:
                        attrs = [firstAttr] + list(restAttrs)
                        funcs.append((f'{target},{scan},{i},({jj(keys)}),({jj(attrs)})',f'PipelineFunctions::runPipeline<{target}, {scan}, {i}, std::index_sequence<{comma(keys)}>, std::index_sequence<{comma(attrs)}>>'))


funcs.sort()

def inst(v):
    return f"template void {v}(TargetBase&, ScanBase&, engine::span<const DefaultProbeParameter>, engine::span<const unsigned>, engine::span<const unsigned>)"

step = (len(funcs) + 15) // 16
for ind, i in enumerate(range(0, len(funcs), step)):
    with open(f"pipeline/PipelineGen{ind}.cpp", "w") as f:
        f.write('#include "pipeline/PipelineGen.hpp"\n')
        f.write('namespace engine {\n')
        for n, v in funcs[i:i + step]:
            f.write(inst(v) + ";\n")
        f.write("}\n")

with open("pipeline/PipelineGen.cpp", "w") as f:
    f.write('#include "pipeline/PipelineFunction.hpp"\n')
    f.write('namespace engine {\n')
    f.write(f'size_t PipelineFunctions::numFunctions = {len(funcs)};\n')
    f.write('std::pair<std::string_view, PipelineFunction> PipelineFunctions::functions[] = {\n')
    for n, v in funcs:
        f.write(f'{{std::string_view{{"{n}"}},&{v}}},\n')
    f.write('};\n')
    f.write('}\n')

print("Number of generated functions:", len(funcs))