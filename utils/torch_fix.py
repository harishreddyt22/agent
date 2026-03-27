"""
torch_fix.py — MUST be imported before any docling/transformers import.
Fixes [WinError 1114] and torch.__spec__ is None errors on Windows.
Torch is not needed locally — all inference runs on Colab GPU.
"""
import sys
import types
import contextlib
import importlib.util


def _apply_torch_fix():
    # If torch already loaded cleanly, nothing to do
    existing = sys.modules.get("torch")
    if existing is not None and getattr(existing, "__spec__", "MISSING") != "MISSING":
        return

    # Try a clean import
    try:
        import torch
        if torch.__spec__ is not None:
            return  # loaded fine
    except Exception:
        pass

    print("⚠️  torch not available or broken — installing mock (inference runs on Colab GPU)")

    # Build a proper mock with __spec__ so docling doesn't crash
    torch_mock = types.ModuleType("torch")
    torch_mock.__version__  = "0.0.0+mock"
    torch_mock.__spec__     = importlib.util.spec_from_loader("torch", loader=None)
    torch_mock.__loader__   = None
    torch_mock.__path__     = []
    torch_mock.__file__     = None
    torch_mock.__package__  = "torch"
    torch_mock.device       = lambda x: x
    torch_mock.float16      = "float16"
    torch_mock.float32      = "float32"
    torch_mock.bfloat16     = "bfloat16"
    torch_mock.no_grad      = contextlib.nullcontext
    torch_mock.Tensor       = object
    torch_mock.Size         = tuple
    torch_mock.long         = int
    torch_mock.bool         = bool

    # torch.cuda mock
    cuda_mock = types.SimpleNamespace(
        is_available     = lambda: False,
        empty_cache      = lambda: None,
        memory_allocated = lambda *a: 0,
        device_count     = lambda: 0,
    )
    torch_mock.cuda = cuda_mock

    # torch.nn mock
    nn_mock = types.ModuleType("torch.nn")
    nn_mock.__spec__   = importlib.util.spec_from_loader("torch.nn", loader=None)
    nn_mock.Module     = object
    nn_mock.Linear     = object
    nn_mock.functional = types.ModuleType("torch.nn.functional")
    torch_mock.nn      = nn_mock

    # torch.backends mock (docling checks this)
    backends_mock = types.SimpleNamespace(
        cudnn = types.SimpleNamespace(enabled=False),
        mps   = types.SimpleNamespace(is_available=lambda: False),
    )
    torch_mock.backends = backends_mock

    # torch.jit mock
    jit_mock = types.ModuleType("torch.jit")
    jit_mock.__spec__ = importlib.util.spec_from_loader("torch.jit", loader=None)
    jit_mock.script   = lambda f: f
    jit_mock.trace    = lambda f, *a, **kw: f
    torch_mock.jit    = jit_mock

    # Register all submodules
    for mod_name in [
        "torch", "torch.cuda", "torch.nn", "torch.nn.functional",
        "torch.jit", "torch.backends", "torch.backends.cudnn",
        "torch.utils", "torch.utils.data",
        "torch.distributed",
        "torch.multiprocessing",
    ]:
        if mod_name not in sys.modules:
            sub = types.ModuleType(mod_name)
            sub.__spec__ = importlib.util.spec_from_loader(mod_name, loader=None)
            sys.modules[mod_name] = sub

    # Override the top-level torch with our full mock
    sys.modules["torch"]      = torch_mock
    sys.modules["torch.cuda"] = cuda_mock
    sys.modules["torch.nn"]   = nn_mock
    sys.modules["torch.jit"]  = jit_mock


_apply_torch_fix()
