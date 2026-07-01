//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2026 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crypto
import Foundation
import SwiftTLS
import Testing
import X509

@testable import NIOQUIC

struct AuthenticationCallbackTest {

    @available(anyAppleOS 26, *)
    @Test("Verify extended key usage allows server auth")
    func callbackCreationChecksExtendedKeyUsageIfAvailable() throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )

        // Key usage bit not set for digitial signatures
        #expect(throws: QUICError.certificateNotSuitableForAuthentication) {
            _ = try Authenticator(certificates: [rootCert], privateKey: rootKey)
        }
        #expect(throws: QUICError.certificateNotSuitableForAuthentication) {
            _ = try Authenticator(certificates: [intermediateCert], privateKey: intermediateKey)
        }

        // extended key usage: server auth
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false,
            extendedKeyUsages: [.serverAuth]
        )
        #expect(throws: Never.self) {
            _ = try Authenticator(certificates: [leafCert], privateKey: leafKey)
        }

        // extended key usage: client auth
        let (leafCertClientAuth, leafKeyClientAuth) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false,
            extendedKeyUsages: [.clientAuth]
        )
        #expect(throws: QUICError.certificateNotSuitableForAuthentication) {
            _ = try Authenticator(certificates: [leafCertClientAuth], privateKey: leafKeyClientAuth)
        }

        // extended key usage: client auth + any
        let (leafCertClientAuthAny, leafKeyClientAuthAny) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false,
            extendedKeyUsages: [.clientAuth, .any]
        )
        #expect(throws: Never.self) {
            _ = try Authenticator(certificates: [leafCertClientAuthAny], privateKey: leafKeyClientAuthAny)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Test externally generated certificates")
    func callbackCreationWithExternallyGeneratedCertificates() throws {
        let certStr1 = """
            -----BEGIN CERTIFICATE-----
            MIIVhjCCFS2gAwIBAgIUHyzb6AHtPTTzq8315j3aF0M8zbEwCgYIKoZIzj0EAwIw
            KDEmMCQGA1UECgwdaW50ZXJvcCBydW5uZXIgaW50ZXJtZWRpYXRlIDIwHhcNMjUx
            MjA1MTYzMTQ3WhcNMjUxMjE1MTYzMTQ3WjAeMRwwGgYDVQQKDBNpbnRlcm9wIHJ1
            bm5lciBsZWFmMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEXX2+T4ggg8nPqbI4
            WbEghNavaUDlA5IoESoe9doVUONmwoYu8lKdsRUK9Lk4I8rs8hpS50CCRMfjjh79
            y3+YRqOCFD0wghQ5MIIT9QYDVR0RBIIT7DCCE+iCBnNlcnZlcoIHc2VydmVyNIIH
            c2VydmVyNoIIc2VydmVyNDaCgfpUVWp0YUpIZjBGN1F4TFl1U01Ma1puZVhjY3d1
            UXBKcjlOU2hsdXE1V053cERZUk1tQTN4aHlYMmdFTDlsVmw2eXh6ajJLNWhYYlZ3
            UlM5YzJtS21HeWNaY3hBaG91M2Q2Z21Bdm5GbHdvWjZxUmt3TE03VU9rTHJ3N3U4
            M01IUnN6V3p6cEIwTENBSGZHT2Z2b1VjNEd2T0JEbFFMemYwSVlaaGk5bFRrU0dT
            clZ5dk5CUld0U01hT3VPTURxTjR4M2FMMjJmQ2xEdHdldmRSOFFUSUZ4MGlNZDNn
            U1NVZk9ydlg3a0tpa0syTHNHend1NDl0VnBNZnVEgoH6aERQSlh0ZXFrNkxVSmFr
            cmVZOU5jVUt2V0htOEhObUJoT0ZvRVFJOHRua211UGpQTWNmNWQyZ2xuZExiY0Ex
            ZVZ2cTNyMTZiUzgzalpCY1NvcThkNkNZbjhHZzdrcTFzQXgwTE91am5DbGwxeHVK
            VTNTUG5wU3k3a3VBbDI0Z0Zrand4NHhpWEtCWm9qQjNLMTg5WFI0Ykhub3N6bDk4
            WGQ5SEEzb255MHM1MDdlNllWVnBZakN4dFdDb29LRkZVZDd1cXdQYkhGWENpamRB
            ZG1pWlpKczFWcE5XREh0enpkMGcweUt4SVdhdmJySzN5WHVBQU9YNnJUYoKB+klh
            MmVER0FHMXlFbVNvMDBpbUQwczVSeUdjNloyQXJMWkVwN1V4Nm0xOHpURjJESExO
            ejIyT2VzTmY4a1B4eVVVQVMwb0JCd3VhZXRrTWlEVURydEFRU2JoZmdvOWNzYlp4
            Sk1lTTliWHhxT042b3BRcU8yREUzZktFY3Vyc1dZQnRXUVN1TmthdDZiQVhGdWQz
            RWZuZUhIREpkQ2hpcjdZNmNJYkRwVTdHZ2ZXZm5KMUNHS3hzbTZsUGJMUTFJeFoz
            ZFFBQmpROTJjUWRKRElkbXdXbEhERlRKYmZSRWI4MUZKa0Q3UEFMdGxPS1ZjT0p3
            M3NTRGlpMUmCgfo5ZjM2TDJMTnVpb1ZDTVcxZUtzMUw3RU5xYXUzSEh4eDUwWEZn
            N3lXT2x1Q0ZKMUg1emhnS0Y5UWtRTHRpbFRJcGZHYVpiZkc3TUF2QTRHV1UwTzhv
            d0JYNUdTS3FWU2JVaDk3Q01QVkNIOWRvYmp3VDd3MnBMMmw4Tk50NDQ2YTh0NUdi
            SlhjVEhEQXlLSGFSeUEyV1gyNHhwakFqcVIxUFFFZG1DVnk3cXZ0dHpXcGU2RXlK
            MllVbGhmZFJXdVRkN3NPQ01ub1NDT3N6UE5OaFd4dGNPWjZYUGREWndsaGlQNlJZ
            MUlXMDhiT1Zscm01YzBCbUZsOW5YgoH6b1ZDcXJQOU0wYUhHMjRxeTljaUg3eFN5
            S2ZUS21pVTF4T1A0cWd4aEtNemxRRDlUMEtRamg4ZU5vYk9ieFJ4QTVua2p0QnU3
            NGVobVM1ZmJuWjFUV3pETGZhZ0EzSUVTbTRHY3MyeEZqaXhpNGM1dHVxWUNiWmpt
            SVNtTmdTUUJOeTlIR2tTdWNiQTVPNldKNUo1VFhVUGhSYjZMc1NLSnJOaUl2bEJw
            bXQwMlNXeU9OTzU1M2xTdGdzZXFlaHpwWGthT3R2b0I5SUtGOVA4UFRWWFdFSFNQ
            elZCUUVySmNpMmxpbmYzRzkxN2VpUTJ4NXFmZUNZTG8wdoKB+jl3cWJJWVBrYTJC
            SFRwa1N3eURsRnVlZjVVUjZyR21jTjNLNjdKT3B6NUQ4bjRLSWl5Q2dEZDZrN2VR
            MlB6V0FQUTZNTlFDeXEwN3c2Unh3VlpFeWR5Tkg4bml4NUc5TWtoTjZ1ek9FNXdq
            c3JIWmxNMXlzdjVnUHJWYkhubGxqZ01ZMTUzQ2VCUDUySHlDMmk5eFFXVkNHVVhL
            bkEyM0U0T1hrRWlJVWIxdktqRzRlcU92bjU4M2lXeVdXMFVtUzc2cHJxclBDM1lJ
            b21QaEJXeU5BTUd4MTRNam5UUGlwaTJEbHFlMEw4bEJUZ09GWFZodnZHWEMwdEuC
            gfp2RDRSVjBJNkttOGNGMzdISXh3MWVUZXJ0cVVveTZKcGJXRnE2RFEwR1BicDh1
            R0tzUFBTdzJQUUI2cXRuMmdIZnhsR0RiN05JQXlvRDNMdVFabmg1OXh3RGpTY0hh
            UUFQMEdEWG1pOFoxWE9teUZ5R2Q2RHRCckdOZzR6NUZXQ3F2eDJZTGJsQ3NuZEd2
            NFM2aVpPS0xVTGJsMmk5VWZENXNGdnNkeFBVOWhJeTczemZ5cTRDQXZjV2ptcWZ6
            MmhxYjZrQ0JLdGxiREY2T1JjWERSZ1E3VXI1SXRTMkR2Vk5GTk14T2NqSkZKRjhQ
            SE1pRTNMb0JwekZBgoH6Q3hZeUZmanBLM29ZSjdaOFhHWWFaa2l1akdTQ3B1UTdP
            Z3V6cG1IbFVDSEtPSTJOU01ETHZQV1BPUFlLaVBSS2dlejVRYkxVeW1ZMFpNaTAw
            VTZpVVFGTVpYR1Q4ZjNmMmg5TEdIM3ZEc1h2clVudnAxZU11dmhhenBCRVNJM3Zo
            UVMyMFB3ODJzUFNxSzFoU1Y3TFZZb25pRHJuNXpjc1Jib1VraGRRN1Voa2EwMnFs
            eWN4VlRBclVYUXFlakpiOGEzQWxCU25Da2xqc0xSM0VpRHhXUUEwS3FtWVp3eW1m
            NEF1NGZWbDRwTGs0cFhrTGd4MEhHa0MzdYKB+ldMZmQySG53cHZ0VEhJWjJvWUMz
            SzRCMTZWV05CUTVGMWxtaFBKb0drQ29LVmc5aVpzOE1EcWMxQ1NTNVp5cFBQN041
            eHQ1cERvS1labmhzNmZBU2s2QTRTYTNHTW9LTzlOa1p1dnVMZnhybUpBZ0tDdThn
            ek5LS3NQUG5idk5maVhVZ21YUWlpQldhMXA5QkVFVU9aNFBrams1eHU1c1psUUZt
            TnVWbE12QnBOZ3VJNmFxOHI0aXg2Nzd2RUNydDA2NVZ6TTBVT2c1RkNqejNMU0xP
            b2lENWx0YzkwMVZzeWFraTdIS2hBWUlQUWJPcEk0Zm9RaDJiSjGCgfpVdERtMVRj
            NHJVdzY2VDRZYTA0Zk56NmlMbFhqbks1WWttUXZCNnYzUGpWdzJwY3RIYUlUakhW
            RWFLQ3lJWVZSdkFUcENYTVdocVNaTk1YT0RNQm1pTzJqdkZ5N2wzcHZwdmZFamlS
            S0JCcUt2aVI0RWVPeE5FWUJxZjYzWno1bVRGMzVCMW1acFlCTjU4aXJkaWd5WFgx
            Mk9Tbzd6NzUyeWxqT0ppQVBPTGVROHBMQ3ZEQVdMYWs4bzNidTZLT1J5TWo4blpz
            T3Q4QTRxQ0RRbU5PUVlLaHNxc0E0SFc0emVUWXB6c1hPZlVmVlcwenhDUHdKVFNo
            ejZ0goH6ak5vSmQ3QzhpaGhUT2ZQSlhIdFpwZzMxcU1aVExvR2dOVEQyOWR0Z0xH
            R2d5WlZaSGZvUkZzaTFjZDE4QjJ6bENhVlFQUWV3cmtkTUtqNkUzRElDMWZQRnJK
            MGRrTEllbGc0V2tIZHVWS3dHOFlhSlV5VTJnZFpPZHhPNDB3cTVMMFlNVnZuVll5
            ZFJtbVFKd3Q3MExYbFFqcVJiZkIwWFliSUVQMXpPZThndWVHWEg5Z0IxeUg5aWpE
            WEx5TEtaRXBtZTJPMFBYSjI4UW9BOW5zcWU3elZ4SE5LTXZnR0dFdVpCNW9hbEtj
            a1NiVGRlR3Bob2dzMEtTTYKB+lk4aXdZVXBDTTNzemlvaUNidkpjcUJ1MkdoUmV3
            ZWZSZllnMkI0R1plNWhpejdhWXlaSk1hZ09MZ2gzczhXQ283RGZSemduc3pWeEYw
            QzhWRTN3NnBqalhlYjFOeWV2NmlNYmVXRXpkckxQUE52TGFNOTRBelJmY3RySGIx
            WmhyTU82cjIyUW1ZMzhxMDcxN3h1NWZOeGdwZGloemlpeUJyWG96d1B5SjJoVFhP
            TFBmNTlXWGZEc00yanRvc3hwd2dIY2J6R3p2RjU0R3FSeU93c2h0eE1lTGEweVln
            UVRTa1I3M0lRTlNOYVJrNWxSS2U4RFBhUmVldHWCgfpkckVPRGJIMkg2N1V0d1k1
            S2ZUd00xV0VrWFdoOWluSVJUZ2p6ZHo0ZU1rdTF3VmdaejdQRWt3QWpuUzdnR0Qz
            VVNHUjNYU25FQXhFaGpQek1xQ0diNjlqNkd3OU1mVHd2Z2xuSHV4R3ltUDhDUHcx
            djAxZERHZllua0VWM1lYQnZCQTRZWFByV1cyRTJndDMwM2VzWmxDTDVPZWs1ZUZt
            akV3M1J3cmljcjJZdFE3VUVHMlRBMllXcGF1N3JyTGQ4UlZROHNHclo1NWdqM0xD
            bDFtU2oxckl4SXZ6TU0za1NaQkhzVGg4Vm8xRkd6YmtlMlByMlJlaVV0goH6RTVX
            UmlqZTUwa1haSm1zUUdOSW5qYm55VVRxQzd0bzgzdHQyaEtzTzBNcE1hSlVtanBS
            d2I3aFJlZWNMSXFJcTJURG9hT2laZExERVhRODMyT2NVanp0VVdjVTlPQnJDZnV1
            STMwcGZmT1FLM1RaS1M0dk5vbHlTeU5uWW9WVWRmcTBpdUJBclY1SUh1RXo2RnFm
            TmNEcVZMbjl4dWVYVWtXNWpSS0ZONGhkSlhTM2ZQUklVeTJmOEh6VDFNYzdvQ2RY
            d1hZZ1VrY2pwNXJxbjJ6aGlQVlc4eVdLaVI0UXZoOTZwSjFldEtscHZBM3VPVzJK
            bk9aNmREaYKB+lNnQlN6dGRKcWx2eDhvWUczSXJSaWtvWE9kWnZ4VEw1eHBmSlFG
            VHlqbG1MM2hyU3djQlVBakczWWxNNlZSb25qek93Z3pYMVkwQmNiRjBnYzFseHQ4
            MUVPWmQ0YUltc2JsMnJ6OEZyTEhFVTg0V29hYnN2MUZUNGRvbW1CamIxTkYybjVj
            a0tNWjF6clZSZThGOG8zSno5WUFqNjVBMG9OcUY1SGpNYktHSnBZWnNnVDF6Qnha
            Z25YWVl2SlJ6c3VMVnRUZTdDOExKbzZ1R29YTVlnSTlkWnF5WU84dzluRGdOaWxv
            dTViaTVGcHJ4aDNIUTZXc3pJZzeCgfpCTmZtTGUySVBZOFpvaTNmeUNjODlPd3A4
            SThHS2ZoNEJyWnE4MjVBNFp1ZnJONkhKcXR4anJvOGl5NFljck82Z0NEZUNzbzRa
            ZVZBTEozcTFOdW9XMGVreTEzQ3djUWdFbWhWZml6RkxHODRyb3dHd2xpUmljeVpo
            bE1PT2doWW1pY1VjZkZxeE9kemJhaVZZNmwzSXczTUFKbVlHWVZtS3pJd1JpOEhX
            T1hreFhMZG03R09ZcWcwcU9hdTkydzhPVW5CYTJWcUgxdDhZUDRKMTI1V2xEb0tC
            T1JYcXVaU2tIRll0R1dZWWU4a1ZYeWdodG5OWUt0OGhvgoH6NE8yTWZZWjFFYnps
            VWt0QWhNbFZvbENPWGpkVThvZHRWZGU3NWNVTVR5aERwaE9QdkVzWUhzZE1LdGdQ
            MWJGUVYyd21Jd1VuMFZnZUllRVY5WGRnWVhMR1ZmTldxdENnYzNLRFhXbERYUFhl
            bWxXWTVaREJXbTV0a2wzUkR3V2FEekJOc3hoZ1oyeWM1TWVFaTFwY0gyQXlBcE1M
            SWozUlRWN2gyNFFjQjBpUU5NV0lrdXlyRGswMmx6aHlpTDQxMVpuUk9xT3ZubjBp
            Z0RHMGZ5aE5UWVlCY3lteDVQb1ZzSGlkc0tkazNSS09WTFBHano5WWtYSkZ3M4KB
            +mVkamllQXd5bFNpbGQyeWxVZ280bDlDc2s1ak5PVjM3dUNzOVpjbFNGUXhyek9z
            TWQ4Y0pLUERBd3E0ajZsWW03MXExU1B6c0pDM1hDM3BGUWJzWEQyNGZBWG83SzZJ
            OWMwUGdNTjBBb0dBZzlXalpqT0VEbFp6enFwb3Y3eTd1T1BDSTA0dUo5cVo3bnpx
            dzlJQTBhMXNneWlkYXFDUFk1enl6bXhxRkVNREpqc1Y3dDg5NGxsSjU5b1NSMmZH
            aVFFNWFuY0pwS0pkR2dKMHlQaWhZWGJLdTBEY0FUejZEMUNaRk5DaVh4TUlaUmta
            YW9UQ0tYQ0N4Q3GCgfoyZmRUTlFGOVpXUTBHNmdBakNCdWZRVThzQkFhR3lrWmZB
            WVNnWmw4dlh4aWJJdVBWNHlPQkMzR2oxOHBqM2lqZ0NXZHdUcjRKSWwxQzdkSDQ4
            VnJqcjhhd1FrSHVyZE9VMUJMZThoWU9LQ3ZFbG9LQWNMTHBsNlZ1UjlYNDlreWcz
            SEYyMU5EQW1wSkpuZGhmcHBMck94eUFFak9YQ0sxVUpjYnZyMmtCT2cySUN6dk1w
            WXNlSHpxaW9wdGpqaUZDQldPWGVyVkhWNkU4dkgySEh4Q1VqSE1za29kTGxJZnBx
            TEdOeU9LbnJ6Mm1JVlBmbEZORmVnWjlSgoH6am9vbjBCRndPdFBwWnV4czh4RmRw
            S203RktuNVlnRXlQc29Yc01ZWkZGV0FJbHN0OXowM2gwY2ZmMVdQSGd1MVlkOWZG
            NGFRUUFuNWRrTG1UUG9JcWlZQnA3S09zc0dtOWhoOHpLdEVXNGpZMXJjdzRSZGpL
            eGl3aGZaN3d1cVQ0MUw0RE9HWUdRNjRuRWQzbTVMT3BFOXhCVmNIWmh1U0x0bEtM
            d3BiWlFkM2QwenVMeUhWYTRiZDdyYXkzVEdMTWtZT25Pd1hOYVdpeFpjSEY3azdC
            R2tXb0pod0ZKTWdjZkZrUVdNUEttYTlIQ2d3SXBpOUFVeXEyWjAdBgNVHQ4EFgQU
            GC4IZ9b6ZpZpvvAOpqtfaKCzndIwHwYDVR0jBBgwFoAUPDtc4DPD3dwy7no8r1O/
            7wRWN4YwCgYIKoZIzj0EAwIDRwAwRAIga1SuXCMvj43w9xv2NnuY0GHNjJT1SehL
            9I6BomnPrEcCICU7tWphjuJSCk/wdi2wnlMtkqCOfD7hTEKv2C0sL1K2
            -----END CERTIFICATE-----
            """
        let certStr2 = """
            -----BEGIN CERTIFICATE-----
            MIICCzCCAbCgAwIBAgIUDo5NnfYnd6vl8jn5jpM4DgYJvQAwCgYIKoZIzj0EAwIw
            KDEmMCQGA1UECgwdaW50ZXJvcCBydW5uZXIgaW50ZXJtZWRpYXRlIDEwHhcNMjUx
            MjA1MTYzMTQ3WhcNMjUxMjE1MTYzMTQ3WjAoMSYwJAYDVQQKDB1pbnRlcm9wIHJ1
            bm5lciBpbnRlcm1lZGlhdGUgMjBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABN3i
            eO676cc0k3OnSfbob5zE8XyuTcHBgdGMGyyANPEQ+Y2eyFCmjHrCDLYhWN4BsNa3
            269bA5Cq7Ep1w5egccqjgbcwgbQwDgYDVR0PAQH/BAQDAgIEMB0GA1UdDgQWBBQ8
            O1zgM8Pd3DLuejyvU7/vBFY3hjBvBgNVHSMEaDBmgBQkRYIvt6lfexTonPF9HpPw
            me7BgKE4pDYwNDEyMDAGA1UECgwpaW50ZXJvcCBydW5uZXIgUm9vdCBDZXJ0aWZp
            Y2F0ZSBBdXRob3JpdHmCFE5QMdNaCSvhow/ITSBTGJPEZBDVMBIGA1UdEwEB/wQI
            MAYBAf8CAWQwCgYIKoZIzj0EAwIDSQAwRgIhAK6a9iNtnCygcA8KhT45zAlmegel
            mf/IES2+ZIkojyCJAiEAnSgV0aoMbLDyBS7Cd4FJ84ZMBT46Wjg3O29DA8A1X6k=
            -----END CERTIFICATE-----
            """
        let certStr3 = """
            -----BEGIN CERTIFICATE-----
            MIICFTCCAbygAwIBAgIUTlAx01oJK+GjD8hNIFMYk8RkENUwCgYIKoZIzj0EAwIw
            NDEyMDAGA1UECgwpaW50ZXJvcCBydW5uZXIgUm9vdCBDZXJ0aWZpY2F0ZSBBdXRo
            b3JpdHkwHhcNMjUxMjA1MTYzMTQ3WhcNMjUxMjE1MTYzMTQ3WjAoMSYwJAYDVQQK
            DB1pbnRlcm9wIHJ1bm5lciBpbnRlcm1lZGlhdGUgMTBZMBMGByqGSM49AgEGCCqG
            SM49AwEHA0IABO4F5Snq/DWhK3MmXEvKv8UzNh/h3Guy7g+OCRJMccRN8apbRq06
            048wT8BzV9WdJobJv+0MAwkqsZ2YGVdZozajgbcwgbQwDgYDVR0PAQH/BAQDAgIE
            MB0GA1UdDgQWBBQkRYIvt6lfexTonPF9HpPwme7BgDBvBgNVHSMEaDBmgBT6ZwhL
            UM73u4UwOxkPxb/4TE5DyKE4pDYwNDEyMDAGA1UECgwpaW50ZXJvcCBydW5uZXIg
            Um9vdCBDZXJ0aWZpY2F0ZSBBdXRob3JpdHmCFF5dwycbcKTMN+87OXS4XFfAXVv7
            MBIGA1UdEwEB/wQIMAYBAf8CAWQwCgYIKoZIzj0EAwIDRwAwRAIgT112FEee9u5A
            djZ9KE2M8R2a5RfCA6/pklWNyyPAXDACIDuDKZqUS5akehxyAT6nr0F3dkOzuzvk
            rYdAUHl20X78
            -----END CERTIFICATE-----
            """

        let cert1 = try Certificate(pemEncoded: certStr1)
        let cert2 = try Certificate(pemEncoded: certStr2)
        let cert3 = try Certificate(pemEncoded: certStr3)

        let keyStr = """
            -----BEGIN EC PRIVATE KEY-----
            MHcCAQEEIB4m61JlCsUHJqHiAw8cddzBvb1FlezumnFhktbgn0FboAoGCCqGSM49
            AwEHoUQDQgAEXX2+T4ggg8nPqbI4WbEghNavaUDlA5IoESoe9doVUONmwoYu8lKd
            sRUK9Lk4I8rs8hpS50CCRMfjjh79y3+YRg==
            -----END EC PRIVATE KEY-----
            """

        let key = try Certificate.PrivateKey(pemEncoded: keyStr)

        // Key usage bit not set for digitial signatures
        #expect(throws: QUICError.certificateNotSuitableForAuthentication) {
            _ = try Authenticator(certificates: [cert3], privateKey: key)
        }
        #expect(throws: QUICError.certificateNotSuitableForAuthentication) {
            _ = try Authenticator(certificates: [cert2, cert3], privateKey: key)
        }

        // Cert intended for server authentication
        #expect(throws: Never.self) {
            _ = try Authenticator(certificates: [cert1, cert2, cert3], privateKey: key)
        }
    }
}
