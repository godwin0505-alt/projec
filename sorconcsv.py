import os
import struct
import crcmod
import logging
from datetime import datetime, timezone
from enum import Enum

# Setup logging
logging.basicConfig(format="%(message)s")
logger = logging.getLogger("sor2csv")
logger.setLevel(logging.INFO)

# Constants
SOL = 299792.458 / 1e6  # Speed of light in km/usec
DIVIDER = "-" * 80

class SorConverter:
    def __init__(self, filename):
        self.filename = filename
        self.results = {}
        self.tracedata = []
        
    def convert_to_csv(self, output_file=None):
        """Main function to convert SOR to CSV"""
        status, results, tracedata = self.sorparse(self.filename)
        if status != "ok":
            logger.error(f"Error parsing SOR file: {status}")
            return False
        
        if not output_file:
            base_name = os.path.splitext(os.path.basename(self.filename))[0]
            output_file = f"{base_name}.csv"
        
        self._write_csv(output_file, tracedata)
        logger.info(f"Successfully converted to {output_file}")
        return True
    
    def _write_csv(self, output_file, tracedata):
        """Write trace data to CSV file"""
        with open(output_file, 'w') as f:
            f.write("Distance (km),Loss (dB)\n")  # CSV header
            for line in tracedata:
                # Convert tab-separated to comma-separated
                csv_line = line.strip().replace('\t', ',')
                f.write(csv_line + '\n')
    
    def sorparse(self, filename):
        """Parse SOR file and return data"""
        try:
            fh = self._sorfile(filename)
            self.results["filename"] = os.path.basename(filename)
            
            # Process map block first
            status = self._process_mapblock(fh)
            if status != "ok":
                return status, None, None
            
            # Process other blocks in order
            klist = sorted(self.results["blocks"], 
                         key=lambda x: self.results["blocks"][x]["order"])
            
            for bname in klist:
                ref = self.results["blocks"][bname]
                bname = ref["name"]
                
                if bname == "GenParams":
                    status = self._process_genparams(fh)
                elif bname == "SupParams":
                    status = self._process_supparams(fh)
                elif bname == "FxdParams":
                    status = self._process_fxdparams(fh)
                elif bname == "DataPts":
                    status = self._process_datapts(fh)
                elif bname == "KeyEvents":
                    status = self._process_keyevents(fh)
                elif bname == "Cksum":
                    status = self._process_cksum(fh)
                
                if status != "ok":
                    break
            
            fh.close()
            return status, self.results, self.tracedata
            
        except Exception as e:
            return f"Error: {str(e)}", None, None
    
    # File handling and basic parsing functions
    class FileHandler:
        """Wrapper for file handle with CRC checksum"""
        def __init__(self, filehandle):
            self.filehandle = filehandle
            self.bufsize = 2048
            self.buffer = b""
            self.spaceleft = self.bufsize
            self.crc16 = crcmod.predefined.Crc("crc-ccitt-false")
        
        def read(self, *args, **kwargs):
            buf = self.filehandle.read(*args, **kwargs)
            xlen = len(buf)
            if xlen > self.spaceleft:
                self.crc16.update(self.buffer)
                self.buffer = b""
                self.spaceleft = self.bufsize
            self.buffer += buf
            self.spaceleft -= xlen
            return buf
        
        def digest(self):
            self.crc16.update(self.buffer)
            return self.crc16.crcValue
        
        def seek(self, *args, **kwargs):
            if args[0] == 0:
                self.buffer = b""
                self.spaceleft = self.bufsize
                self.crc16 = crcmod.predefined.Crc("crc-ccitt-false")
            return self.filehandle.seek(*args, **kwargs)
        
        def tell(self):
            return self.filehandle.tell()
        
        def close(self):
            return self.filehandle.close()
    
    def _sorfile(self, filename):
        """Open SOR file with our custom handler"""
        try:
            fh = open(filename, "rb")
            return self.FileHandler(fh)
        except IOError as e:
            logger.error(f"Failed to read {filename}")
            raise e
    
    def _get_string(self, fh):
        """Read null-terminated string from file"""
        mystr = b""
        byte = fh.read(1)
        while byte != b"":
            tt = struct.unpack("c", byte)[0]
            if tt == b"\x00":
                break
            mystr += tt
            byte = fh.read(1)
        return mystr.decode("utf-8")
    
    def _get_uint(self, fh, nbytes=2):
        """Read unsigned integer (little endian)"""
        word = fh.read(nbytes)
        if nbytes == 2:
            return struct.unpack("<H", word)[0]
        elif nbytes == 4:
            return struct.unpack("<I", word)[0]
        elif nbytes == 8:
            return struct.unpack("<Q", word)[0]
        else:
            raise ValueError(f"Invalid number of bytes {nbytes}")
    
    def _get_signed(self, fh, nbytes=2):
        """Read signed integer (little endian)"""
        word = fh.read(nbytes)
        if nbytes == 2:
            return struct.unpack("<h", word)[0]
        elif nbytes == 4:
            return struct.unpack("<i", word)[0]
        elif nbytes == 8:
            return struct.unpack("<q", word)[0]
        else:
            raise ValueError(f"Invalid number of bytes {nbytes}")
    
    # Block processing functions
    def _process_mapblock(self, fh):
        """Process the Map block which describes the file structure"""
        fh.seek(0)
        
        tt = self._get_string(fh)
        if tt == "Map":
            self.results["format"] = 2
            logger.debug("Bellcore 2.x version")
        else:
            self.results["format"] = 1
            logger.debug("Bellcore 1.x version")
            fh.seek(0)
        
        self.results["version"] = "%.2f" % (self._get_uint(fh, 2) * 0.01)
        
        self.results["mapblock"] = {
            "nbytes": self._get_uint(fh, 4),
            "nblocks": self._get_uint(fh, 2) - 1
        }
        
        self.results["blocks"] = {}
        startpos = self.results["mapblock"]["nbytes"]
        
        for i in range(self.results["mapblock"]["nblocks"]):
            bname = self._get_string(fh)
            bver = "%.2f" % (self._get_uint(fh, 2) * 0.01)
            bsize = self._get_uint(fh, 4)
            
            self.results["blocks"][bname] = {
                "name": bname,
                "version": bver,
                "size": bsize,
                "pos": startpos,
                "order": i
            }
            startpos += bsize
        
        return "ok"
    
    def _process_genparams(self, fh):
        """Process General Parameters block"""
        bname = "GenParams"
        hsize = len(bname) + 1
        ref = self.results["blocks"][bname]
        fh.seek(ref["pos"])
        
        if self.results["format"] == 2:
            mystr = fh.read(hsize).decode("ascii")
            if mystr != bname + "\0":
                logger.error(f"Incorrect header {mystr}")
                return "nok"
        
        self.results[bname] = {}
        xref = self.results[bname]
        
        lang = fh.read(2).decode("ascii")
        xref["language"] = lang
        
        if self.results["format"] == 1:
            fields = [
                "cable ID", "fiber ID", "wavelength", "location A", "location B",
                "cable code/fiber type", "build condition", "user offset",
                "operator", "comments"
            ]
        else:
            fields = [
                "cable ID", "fiber ID", "fiber type", "wavelength", "location A",
                "location B", "cable code/fiber type", "build condition",
                "user offset", "user offset distance", "operator", "comments"
            ]
        
        for field in fields:
            if field == "build condition":
                xstr = fh.read(2).decode("ascii")
            elif field in ("wavelength", "fiber type"):
                val = self._get_uint(fh, 2)
                xstr = f"{val} nm" if field == "wavelength" else str(val)
            elif field in ("user offset", "user offset distance"):
                val = self._get_signed(fh, 4)
                xstr = str(val)
            else:
                xstr = self._get_string(fh)
            
            xref[field] = xstr
        
        return "ok"
    
    def _process_supparams(self, fh):
        """Process Supplier Parameters block"""
        bname = "SupParams"
        hsize = len(bname) + 1
        ref = self.results["blocks"][bname]
        fh.seek(ref["pos"])
        
        if self.results["format"] == 2:
            mystr = fh.read(hsize).decode("ascii")
            if mystr != bname + "\0":
                logger.error(f"Incorrect header {mystr}")
                return "nok"
        
        self.results[bname] = {}
        xref = self.results[bname]
        
        fields = [
            "supplier", "OTDR", "OTDR S/N", "module", "module S/N", 
            "software", "other"
        ]
        
        for field in fields:
            xref[field] = self._get_string(fh)
        
        return "ok"
    
    def _process_fxdparams(self, fh):
        """Process Fixed Parameters block"""
        bname = "FxdParams"
        hsize = len(bname) + 1
        ref = self.results["blocks"][bname]
        fh.seek(ref["pos"])
        
        if self.results["format"] == 2:
            mystr = fh.read(hsize).decode("ascii")
            if mystr != bname + "\0":
                logger.error(f"Incorrect header {mystr}")
                return "nok"
        
        self.results[bname] = {}
        xref = self.results[bname]
        
        # Process fields based on format version
        if self.results["format"] == 1:
            plist = [
                ["date/time", 0, 4, "v", "", "", ""],
                ["unit", 4, 2, "s", "", "", ""],
                ["wavelength", 6, 2, "v", 0.1, 1, "nm"],
                ["acquisition offset", 8, 4, "i", "", "", ""],
                ["number of pulse width entries", 12, 2, "v", "", "", ""],
                ["pulse width", 14, 2, "v", "", 0, "ns"],
                ["sample spacing", 16, 4, "v", 1e-8, "", "usec"],
                ["num data points", 20, 4, "v", "", "", ""],
                ["index", 24, 4, "v", 1e-5, 6, ""],
                ["BC", 28, 2, "v", -0.1, 2, "dB"],
                ["num averages", 30, 4, "v", "", "", ""],
                ["range", 34, 4, "v", 2e-5, 6, "km"],
                ["front panel offset", 38, 4, "i", "", "", ""],
                ["noise floor level", 42, 2, "v", "", "", ""],
                ["noise floor scaling factor", 44, 2, "i", "", "", ""],
                ["power offset first point", 46, 2, "v", "", "", ""],
                ["loss thr", 48, 2, "v", 0.001, 3, "dB"],
                ["refl thr", 50, 2, "v", -0.001, 3, "dB"],
                ["EOT thr", 52, 2, "v", 0.001, 3, "dB"]
            ]
        else:
            plist = [
                ["date/time", 0, 4, "v", "", "", ""],
                ["unit", 4, 2, "s", "", "", ""],
                ["wavelength", 6, 2, "v", 0.1, 1, "nm"],
                ["acquisition offset", 8, 4, "i", "", "", ""],
                ["acquisition offset distance", 12, 4, "i", "", "", ""],
                ["number of pulse width entries", 16, 2, "v", "", "", ""],
                ["pulse width", 18, 2, "v", "", 0, "ns"],
                ["sample spacing", 20, 4, "v", 1e-8, "", "usec"],
                ["num data points", 24, 4, "v", "", "", ""],
                ["index", 28, 4, "v", 1e-5, 6, ""],
                ["BC", 32, 2, "v", -0.1, 2, "dB"],
                ["num averages", 34, 4, "v", "", "", ""],
                ["averaging time", 38, 2, "v", 0.1, 0, "sec"],
                ["range", 40, 4, "v", 2e-5, 6, "km"],
                ["acquisition range distance", 44, 4, "i", "", "", ""],
                ["front panel offset", 48, 4, "i", "", "", ""],
                ["noise floor level", 52, 2, "v", "", "", ""],
                ["noise floor scaling factor", 54, 2, "i", "", "", ""],
                ["power offset first point", 56, 2, "v", "", "", ""],
                ["loss thr", 58, 2, "v", 0.001, 3, "dB"],
                ["refl thr", 60, 2, "v", -0.001, 3, "dB"],
                ["EOT thr", 62, 2, "v", 0.001, 3, "dB"],
                ["trace type", 64, 2, "s", "", "", ""],
                ["X1", 66, 4, "i", "", "", ""],
                ["Y1", 70, 4, "i", "", "", ""],
                ["X2", 74, 4, "i", "", "", ""],
                ["Y2", 78, 4, "i", "", "", ""]
            ]
        
        # Process each field
        for field in plist:
            name, pos, size, ftype, scale, dgt, unit = field
            fh.seek(ref["pos"] + pos)
            
            if ftype == "i":
                val = self._get_signed(fh, size)
                xstr = str(val)
            elif ftype == "v":
                val = self._get_uint(fh, size)
                if scale:
                    val *= scale
                xstr = f"{val:.{dgt}f}" if dgt else str(val)
            elif ftype == "s":
                xstr = fh.read(size).decode("utf-8")
            else:
                xstr = ""
            
            if name == "date/time":
                xstr = datetime.fromtimestamp(val, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            elif name == "unit":
                unit_map = {"mt": "meters", "km": "kilometers", "mi": "miles", "kf": "kilo-ft"}
                xstr += f" ({unit_map.get(xstr, 'unknown')})"
            
            xref[name] = f"{xstr} {unit}" if unit else xstr
        
        # Calculate resolution and range
        ior = float(xref["index"])
        ss = float(xref["sample spacing"].split()[0])
        dx = ss * SOL / ior
        xref["resolution"] = dx * 1000.0  # in meters
        xref["range"] = dx * int(xref["num data points"])
        
        return "ok"
    
    def _process_datapts(self, fh):
        """Process Data Points block - contains the actual trace data"""
        bname = "DataPts"
        hsize = len(bname) + 1
        ref = self.results["blocks"][bname]
        fh.seek(ref["pos"])
        
        if self.results["format"] == 2:
            mystr = fh.read(hsize).decode("ascii")
            if mystr != bname + "\0":
                logger.error(f"Incorrect header {mystr}")
                return "nok"
        
        self.results[bname] = {}
        xref = self.results[bname]
        xref["_datapts_params"] = {"xscaling": 1, "offset": "STV"}
        
        # Read header
        N = self._get_uint(fh, 4)
        xref["num data points"] = N
        
        val = self._get_signed(fh, 2)
        xref["num traces"] = val
        if val > 1:
            logger.warning(f"Cannot handle multiple traces ({val}); using first trace")
        
        val = self._get_uint(fh, 4)  # num data points again
        val = self._get_uint(fh, 2)
        scaling_factor = val / 1000.0
        xref["scaling factor"] = scaling_factor
        
        # Read data points
        dlist = [self._get_uint(fh, 2) for _ in range(N)]
        ymax = max(dlist)
        ymin = min(dlist)
        fs = 0.001 * scaling_factor
        
        # Convert to dB and store in tracedata
        offset = xref["_datapts_params"]["offset"]
        xscaling = xref["_datapts_params"]["xscaling"]
        dx = self.results["FxdParams"]["resolution"] / 1000.0  # in km
        
        if offset == "STV":
            nlist = [(ymax - x) * fs for x in dlist]
        elif offset == "AFL":
            nlist = [(ymin - x) * fs for x in dlist]
        else:
            nlist = [-x * fs for x in dlist]
        
        for i in range(N):
            x = dx * i * xscaling / 1000.0  # distance in km
            self.tracedata.append(f"{x:.6f}\t{nlist[i]:.6f}\n")
        
        return "ok"
    
    def _process_keyevents(self, fh):
        """Process Key Events block (not used for CSV output but parsed for completeness)"""
        bname = "KeyEvents"
        hsize = len(bname) + 1
        ref = self.results["blocks"][bname]
        fh.seek(ref["pos"])
        
        if self.results["format"] == 2:
            mystr = fh.read(hsize).decode("ascii")
            if mystr != bname + "\0":
                logger.error(f"Incorrect header {mystr}")
                return "nok"
        
        self.results[bname] = {}
        xref = self.results[bname]
        
        # Number of events
        nev = self._get_uint(fh, 2)
        xref["num events"] = nev
        
        factor = 1e-4 * SOL / float(self.results["FxdParams"]["index"])
        
        # Process each event
        for j in range(nev):
            event_id = self._get_uint(fh, 2)
            dist = self._get_uint(fh, 4) * factor
            slope = self._get_signed(fh, 2) * 0.001
            splice = self._get_signed(fh, 2) * 0.001
            refl = self._get_signed(fh, 4) * 0.001
            xtype = fh.read(8).decode("ascii")
            
            if self.results["format"] == 2:
                # Skip additional fields in version 2
                fh.read(20)  # 5 fields * 4 bytes each
            
            comments = self._get_string(fh)
            
            event_key = f"event {j+1}"
            xref[event_key] = {
                "type": xtype,
                "distance": f"{dist:.3f}",
                "slope": f"{slope:.3f}",
                "splice loss": f"{splice:.3f}",
                "refl loss": f"{refl:.3f}",
                "comments": comments
            }
        
        # Process summary
        total = self._get_signed(fh, 4) * 0.001
        loss_start = self._get_signed(fh, 4) * factor
        loss_finish = self._get_uint(fh, 4) * factor
        orl = self._get_uint(fh, 2) * 0.001
        orl_start = self._get_signed(fh, 4) * factor
        orl_finish = self._get_uint(fh, 4) * factor
        
        xref["Summary"] = {
            "total loss": float(f"{total:.3f}"),
            "ORL": float(f"{orl:.3f}"),
            "loss start": float(f"{loss_start:.6f}"),
            "loss end": float(f"{loss_finish:.6f}"),
            "ORL start": float(f"{orl_start:.6f}"),
            "ORL finish": float(f"{orl_finish:.6f}")
        }
        
        return "ok"
    
    def _process_cksum(self, fh):
        """Process Checksum block (verification)"""
        bname = "Cksum"
        hsize = len(bname) + 1
        ref = self.results["blocks"][bname]
        fh.seek(ref["pos"])
        
        if self.results["format"] == 2:
            mystr = fh.read(hsize).decode("ascii")
            if mystr != bname + "\0":
                logger.error(f"Incorrect header {mystr}")
                return "nok"
        
        self.results[bname] = {}
        xref = self.results[bname]
        
        digest = fh.digest()
        csum = self._get_uint(fh, 2)
        
        xref["checksum_ours"] = digest
        xref["checksum"] = csum
        xref["match"] = digest == csum
        
        return "ok"

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert SOR files to CSV")
    parser.add_argument("sor_file", help="Input .sor file to convert")
    parser.add_argument("-o", "--output", help="Output CSV filename (default: <input>.csv)")
    args = parser.parse_args()
    
    converter = SorConverter(args.sor_file)
    if converter.convert_to_csv(args.output):
        logger.info("Conversion completed successfully")
    else:
        logger.error("Conversion failed")